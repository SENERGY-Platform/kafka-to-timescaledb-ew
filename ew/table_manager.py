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
import ew_lib
import psycopg2


class TableManager:
    def __init__(self, db_conn: psycopg2._psycopg.connection, filter_client: ew_lib.FilterClient):
        self.__db_conn = db_conn
        self.__filter_client = filter_client

    def _execute_query(self, query: str):
        with self.__db_conn.cursor() as cursor:
            cursor.execute(query=query)
        self.__db_conn.commit()

    def create_table(self, export_id):
        export_args = self.__filter_client.handler.get_filter_args(id=export_id)
        self._execute_query(
            query=gen_create_table_query(
                name=export_args[ExportArgs.table_name],
                columns=export_args[ExportArgs.table_columns]
            )
        )

    def drop_table(self, export_id):
        export_args = self.__filter_client.handler.get_filter_args(id=export_id)
        self._execute_query(
            query=gen_drop_table_query(name=export_args[ExportArgs.table_name])
        )
