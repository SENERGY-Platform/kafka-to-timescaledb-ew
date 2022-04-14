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

import mf_lib.exceptions

from .util import *
from .model import *
import util
import ew_lib
import psycopg2
import threading
import queue


class TableManager:
    __log_msg_prefix = "table manager"
    __log_err_msg_prefix = f"{__log_msg_prefix} error"

    def __init__(self, db_conn: psycopg2._psycopg.connection, filter_client: ew_lib.FilterClient, distributed_hypertables: bool = False, hypertable_replication_factor: int = 2, timeout: int = 1, retries: int = 2, retry_delay: int = 2):
        self.__db_conn = db_conn
        self.__filter_client = filter_client
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
                func, export_id = self.__queue.get(timeout=self.__timeout)
                try:
                    func(export_id)
                except Exception as ex:
                    util.logger.critical(f"{TableManager.__log_err_msg_prefix}: {ex}")
                    self.__stop = True
            except queue.Empty:
                pass

    def _execute_stmt(self, stmt: str, retry=0):
        try:
            with self.__db_conn.cursor() as cursor:
                cursor.execute(query=stmt)
            self.__db_conn.commit()
        except (psycopg2.InterfaceError, psycopg2.OperationalError, psycopg2.InternalError):
            if retry < self.__retries:
                self.__sleeper.wait(self.__retry_delay)
                if not self.__stop:
                    self._execute_stmt(stmt=stmt, retry=retry + 1)
            else:
                raise

    def _create_table(self, export_id):
        try:
            export_args = self.__filter_client.handler.get_filter_args(id=export_id)
            stmt = gen_create_table_stmt(
                name=export_args[ExportArgs.table_name],
                columns=export_args[ExportArgs.table_columns]
            )
            if self.__distributed_hypertables:
                stmt += gen_create_hypertable_stmt(
                    name=export_args[ExportArgs.table_name],
                    time_column=export_args[ExportArgs.time_column]
                )
                stmt += gen_set_replication_factor_stmt(
                    name=export_args[ExportArgs.table_name],
                    factor=self.__hypertable_replication_factor
                )
            self._execute_stmt(stmt=stmt)
        except mf_lib.exceptions.UnknownFilterIDError:
            pass
        except Exception as ex:
            raise CreateTableError(export_id, ex)

    def _drop_table(self, export_id):
        try:
            export_args = self.__filter_client.handler.get_filter_args(id=export_id)
            self._execute_stmt(
                stmt=gen_drop_table_stmt(name=export_args[ExportArgs.table_name])
            )
        except mf_lib.exceptions.UnknownFilterIDError:
            pass
        except Exception as ex:
            raise DropTableError(export_id, ex)

    def create_table(self, export_id):
        self.__queue.put((self._create_table, export_id))

    def drop_table(self, export_id):
        self.__queue.put((self._drop_table, export_id))

    def start(self):
        self.__thread.start()

    def stop(self):
        self.__stop = True
        self.__sleeper.set()

    def is_alive(self) -> bool:
        return self.__thread.is_alive()

    def join(self):
        self.__thread.join()
