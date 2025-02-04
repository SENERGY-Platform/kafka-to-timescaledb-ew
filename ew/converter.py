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

import datetime


def to_float(val, *args):
    if val is None:
        return val
    return float(val)


def to_int(val, *args):
    if val is None:
        return val
    return int(val)


def to_bool(val, *args):
    return bool(val)


def to_str(val, *args):
    if val is None:
        return val
    return str(val)


def to_datetime(val, fmt: str):
    if fmt == "unix":
        time_obj = datetime.datetime.fromtimestamp(val)
    else:
        time_obj = datetime.datetime.strptime(val, fmt)
    return time_obj


type_map = {
    "TIMESTAMP": to_datetime,
    "TIMESTAMPTZ": to_datetime,
    "bool": to_bool,
    "real": to_float,
    "double": to_float,
    "smallint": to_int,
    "integer": to_int,
    "bigint": to_int,
    "varchar": to_str,
    "text": to_str
}