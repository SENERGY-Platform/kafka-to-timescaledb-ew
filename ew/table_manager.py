"""
   Copyright 2022 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

__all__ = ("TableManager", )

from .util import *
from .model import *
import util
import ew_lib
import psycopg2
import threading
import queue
import confluent_kafka
import json
import mf_lib.exceptions


class TableManager:
    __log_msg_prefix = "table manager"
    __log_err_msg_prefix = f"{__log_msg_prefix} error"

    def __init__(self, db_conn: psycopg2._psycopg.connection, filter_client: ew_lib.FilterClient, kafka_producer: typing.Optional[confluent_kafka.Producer] = None, metrics_topic: typing.Optional[str] = None, distributed_hypertables: bool = False, hypertable_replication_factor: int = 2, timeout: int = 1, retries: int = 2, retry_delay: int = 2):
        self.__db_conn = db_conn
        self.__filter_client = filter_client
        self.__kafka_producer = kafka_producer
        self.__metrics_topic = metrics_topic
        self.__distributed_hypertables = distributed_hypertables
        self.__hypertable_replication_factor = hypertable_replication_factor
        self.__timeout = timeout
        self.__retries = retries
        self.__retry_delay = retry_delay
        self.__queue = queue.Queue()
        self.__sleeper = threading.Event()
        self.__thread = threading.Thread(target=self._run, daemon=True)
        self.__stop = False

    def _run(self):
        while not self.__stop:
            try:
                func, args = self.__queue.get(timeout=self.__timeout)
                try:
                    func(**args)
                except Exception as ex:
                    util.logger.critical(f"{TableManager.__log_err_msg_prefix}: {ex}")
                    self.__stop = True
            except queue.Empty:
                pass
        if self.__kafka_producer:
            self.__kafka_producer.flush()

    def _execute_stmt(self, stmt: str, commit=False, retry=0):
        util.logger.debug(f"{TableManager.__log_msg_prefix}: executing statement: statement='{stmt}' retries={self.__retries - retry}")
        try:
            rows = None
            with self.__db_conn.cursor() as cursor:
                cursor.execute(query=stmt)
                if cursor.rowcount > 0:
                    rows = cursor.fetchall()
            if commit:
                self.__db_conn.commit()
            return rows
        except (psycopg2.InterfaceError, psycopg2.OperationalError, psycopg2.InternalError, psycopg2.DatabaseError) as ex:
            if retry < self.__retries:
                util.logger.warning(f"{TableManager.__log_err_msg_prefix}: executing statement failed: reason={get_exception_str(ex)} statement='{stmt}' retries={self.__retries - retry}")
                self.__db_conn.reset()
                self.__sleeper.wait(self.__retry_delay)
                if not self.__stop:
                    self._execute_stmt(stmt=stmt, commit=commit, retry=retry + 1)
            else:
                raise

    def _create_table(self, export_id):
        try:
            export_args = self.__filter_client.handler.get_filter_args(id=export_id)
            if not self._table_exists(name=export_args[ExportArgs.table_name]):
                if self.__kafka_producer:
                    self._publish_metric("put", [export_args[ExportArgs.table_name]])
                self._execute_stmt(
                    stmt=gen_create_table_stmt(
                        name=export_args[ExportArgs.table_name],
                        columns=export_args[ExportArgs.table_columns],
                        unique_col=export_args[ExportArgs.time_column] if export_args.get(ExportArgs.time_unique) is True else None,
                    ),
                    commit=True
                )
                self._execute_stmt(
                    stmt=gen_create_hypertable_stmt(
                        name=export_args[ExportArgs.table_name],
                        time_column=export_args[ExportArgs.time_column],
                        is_distributed=self.__distributed_hypertables
                    ),
                    commit=True
                )
                if self.__distributed_hypertables:
                    self._execute_stmt(
                        stmt=gen_set_replication_factor_stmt(
                            name=export_args[ExportArgs.table_name],
                            factor=self.__hypertable_replication_factor
                        ),
                        commit=True
                    )
        except mf_lib.exceptions.UnknownFilterIDError:
            pass
        except Exception as ex:
            raise CreateTableError(export_id, ex)

    def _drop_table(self, export_id, export_args):
        try:
            if self.__kafka_producer:
                self._publish_metric("delete", [export_args[ExportArgs.table_name]])
            self._execute_stmt(
                stmt=gen_drop_table_stmt(name=export_args[ExportArgs.table_name]),
                commit=True
            )
        except mf_lib.exceptions.UnknownFilterIDError:
            pass
        except Exception as ex:
            raise DropTableError(export_id, ex)

    def _table_exists(self, name):
        return all(self._execute_stmt(gen_select_exists_table_stmt(name))[0])

    def _publish_metric(self, method: str, tables: typing.List[str]):
        try:
            self.__kafka_producer.produce(topic=self.__metrics_topic, value=json.dumps({"method": method, "tables": tables}))
        except Exception as ex:
            util.logger.warning(f"{TableManager.__log_err_msg_prefix}: publishing metric failed: reason={get_exception_str(ex)} method='{method}' tables='{tables}'")

    def create_table(self, export_id):
        self.__queue.put((self._create_table, {"export_id": export_id}))

    def drop_table(self, export_id):
        try:
            self.__queue.put((self._drop_table, {"export_id": export_id, "export_args": self.__filter_client.handler.get_filter_args(id=export_id)}))
        except mf_lib.exceptions.UnknownFilterIDError:
            pass

    def start(self):
        self.__thread.start()

    def stop(self):
        self.__stop = True
        self.__sleeper.set()

    def is_alive(self) -> bool:
        return self.__thread.is_alive()

    def join(self):
        self.__thread.join()
