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

from .config import *
from .logger import *
import math


def print_init(name, git_info_file):
    lines = list()
    l_len = len(name)
    with open(git_info_file, "r") as file:
        for line in file:
            key, value = line.strip().split("=")
            line = f"{key}: {value}"
            lines.append(line)
            if len(line) > l_len:
                l_len = len(line)
    if len(name) < l_len:
        l_len = math.ceil((l_len - len(name) - 2) / 2)
        print("*" * l_len + f" {name} " + "*" * l_len)
        l_len = 2 * l_len + len(name) + 2
    else:
        print(name)
    for line in lines:
        print(line)
    print("*" * l_len)
