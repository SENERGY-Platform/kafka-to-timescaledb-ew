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

from .model import *
from .converter import *
import base64
import traceback
import typing


def get_exception_str(ex):
    return "[" + ", ".join([item.strip().replace("\n", " ") for item in traceback.format_exception_only(type(ex), ex)]) + "]"


class WriteRowsError(Exception):
    def __init__(self, row_count, export_id, ex):
        super().__init__(f"writing rows failed: reason={get_exception_str(ex)} row_count={row_count} export_id={export_id}")


class ValidateFilterError(Exception):
    def __init__(self, ex):
        super().__init__(get_exception_str(ex))


class TableManagerError(Exception):
    def __init__(self, action, export_id, ex):
        super().__init__(f"{action} table failed: reason={get_exception_str(ex)} export_id={export_id}")


class CreateTableError(TableManagerError):
    def __init__(self, export_id, ex):
        super().__init__('create', export_id, ex)


class DropTableError(TableManagerError):
    def __init__(self, export_id, ex):
        super().__init__('drop', export_id, ex)


def validate_filter(filter: dict):
    try:
        cols = [i[0] for i in filter["args"][ExportArgs.table_columns]]
        for key in filter["mappings"]:
            key, typ = key.split(":")
            if typ == "data" and key not in cols:
                return False
        for i in filter["args"][ExportArgs.table_columns]:
            if i[1] not in type_map:
                return False
        if not filter["args"][ExportArgs.time_column] in cols:
            return False
        return True
    except Exception as ex:
        print(ex)
        raise ValidateFilterError(ex)


def get_short_id(value: str):
    value = value.rsplit(":", 1)[-1].replace("-", "")
    return base64.urlsafe_b64encode(bytes.fromhex(value)).rstrip(b"=").decode()


def gen_create_table_stmt(name: str, columns: typing.List):
    return "CREATE TABLE \"{}\" ({});".format(name, ", ".join(f"\"{i[0]}\" {' '.join(i[1:])}" for i in columns))


def gen_insert_into_table_stmt(name, columns):
    return "INSERT INTO \"{}\" ({}) VALUES %s".format(name, ", ".join(f"\"{i}\"" for i in columns))


def gen_drop_table_stmt(name: str):
    return f"DROP TABLE IF EXISTS \"{name}\""


def gen_create_hypertable_stmt(name: str, time_column):
    return f"SELECT create_distributed_hypertable('\"{name}\"', '{time_column}');"


def gen_set_replication_factor_stmt(name: str, factor: int):
    return f"SELECT set_replication_factor('\"{name}\"', {factor});"


def gen_row(data, columns: typing.List, time_format):
    return tuple(type_map[i[1]](data[i[0]], time_format) for i in columns if i[0] in data)
