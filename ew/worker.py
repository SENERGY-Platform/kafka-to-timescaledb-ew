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

__all__ = ("ExportWorker", )

from .util import *
from .model import *
import util
import ew_lib
import mf_lib
import threading
import typing
import logging
import psycopg2
import psycopg2.extras


class ExportWorker:
    __log_msg_prefix = "export worker"
    __log_err_msg_prefix = f"{__log_msg_prefix} error"

    def __init__(self, db_conn: psycopg2._psycopg.connection, data_client: ew_lib.DataClient, filter_client: ew_lib.FilterClient, get_data_timeout: float = 5.0, get_data_limit: int = 10000, page_size: int = 100):
        self.__db_conn = db_conn
        self.__data_client = data_client
        self.__filter_client = filter_client
        self.__filter_sync_event = threading.Event()
        self.__get_data_timeout = get_data_timeout
        self.__get_data_limit = get_data_limit
        self.__page_size = page_size
        self.__filter_sync_err = False
        self.__stop = False
        self.__stopped = False

    def _gen_rows_batch(self, exports_batch: typing.List[mf_lib.FilterResult]):
        batches = dict()
        for result in exports_batch:
            if result.ex:
                util.logger.error(f"{ExportWorker.__log_err_msg_prefix}: generating rows failed: reason={get_exception_str(result.ex)} export_ids={result.filter_ids}")
            else:
                for export_id in result.filter_ids:
                    try:
                        export_args = self.__filter_client.handler.get_filter_args(id=export_id)
                        table_name = export_args[ExportArgs.table_name]
                        table_columns = export_args[ExportArgs.table_columns]
                        row_cols = tuple(i[0] for i in table_columns if i[0] in result.data)
                        row_data = gen_row(
                            data=result.data,
                            columns=table_columns,
                            time_format=export_args.get(ExportArgs.time_format)
                        )
                        if table_name not in batches:
                            batches[table_name] = (
                                export_id,
                                export_args[ExportArgs.time_column] if export_args.get(ExportArgs.time_unique) is True else None,
                                [(row_cols, [row_data])]
                            )
                        else:
                            if row_cols != batches[table_name][2][-1][0]:
                                batches[table_name][2].append((row_cols, [row_data]))
                            else:
                                batches[table_name][2][-1][1].append(row_data)
                    except Exception as ex:
                        util.logger.error(f"{ExportWorker.__log_err_msg_prefix}: generating row failed: reason={get_exception_str(ex)} export_id={export_id}")
        for table_name, item in batches.items():
            if item[1]:
                batches[table_name] = (item[0], item[1], remove_duplicates_from_batch(item[1], item[2]))
        return batches

    def _write_rows(self, rows_batch: typing.Dict):
        if util.logger.level == logging.DEBUG:
            rows_total = 0
            for v in rows_batch.values():
                for b in v[2]:
                    rows_total += len(b[1])
            util.logger.debug(f"{ExportWorker.__log_msg_prefix}: writing rows: row_count={rows_total}")
        with self.__db_conn.cursor() as cursor:
            for table_name, item in rows_batch.items():
                for batch in item[2]:
                    try:
                        psycopg2.extras.execute_values(
                            cur=cursor,
                            sql=gen_insert_into_table_stmt(name=table_name, columns=batch[0], unique_col=item[1]),
                            argslist=batch[1],
                            page_size=self.__page_size
                        )
                    except (psycopg2.InterfaceError, psycopg2.OperationalError, psycopg2.InternalError) as ex:
                        raise WriteRowsError(len(batch[1]), item[0], ex)
                    except Exception as ex:
                        util.logger.error(f"{ExportWorker.__log_err_msg_prefix}: {WriteRowsError(len(batch[1]), item[0], ex)}")
        self.__db_conn.commit()

    def set_filter_sync(self, err: bool):
        self.__filter_sync_err = err
        self.__filter_sync_event.set()

    def stop(self):
        self.__stop = True

    def is_alive(self):
        return not self.__stopped

    def run(self):
        util.logger.info(f"{ExportWorker.__log_msg_prefix}: waiting for filter synchronisation ...")
        self.__filter_sync_event.wait()
        if not self.__filter_sync_err:
            util.logger.info(f"{ExportWorker.__log_msg_prefix}: starting export consumption ...")
            while not self.__stop:
                try:
                    exports_batch = self.__data_client.get_exports_batch(
                        timeout=self.__get_data_timeout,
                        limit=self.__get_data_limit,
                        data_ignore_missing_keys=True
                    )
                    if exports_batch:
                        if exports_batch[1]:
                            raise RuntimeError(set(str(ex) for ex in exports_batch[1]))
                        if exports_batch[0]:
                            self._write_rows(rows_batch=self._gen_rows_batch(exports_batch=exports_batch[0]))
                            self.__data_client.store_offsets()
                except WriteRowsError as ex:
                    util.logger.critical(f"{ExportWorker.__log_err_msg_prefix}: {ex}")
                    self.__stop = True
                except Exception as ex:
                    util.logger.critical(f"{ExportWorker.__log_err_msg_prefix}: consuming exports failed: reason={get_exception_str(ex)}")
                    self.__stop = True
        self.__stopped = True


def remove_duplicates_from_batch(unique_col, data):
    time_set = set()
    new_batch = list()
    for i in range(1, len(data) + 1):
        row_cols = data[-i][0]
        row_data = data[-i][1]
        t_pos = row_cols.index(unique_col)
        new_row_data = list()
        for x in range(1, len(row_data) + 1):
            row_data_item = row_data[-x]
            timestamp = row_data_item[t_pos]
            if not timestamp in time_set:
                new_row_data.append(row_data_item)
                time_set.add(timestamp)
        if len(new_row_data) > 0:
            new_row_data.reverse()
            new_batch.append((row_cols, new_row_data))
    new_batch.reverse()
    return new_batch
