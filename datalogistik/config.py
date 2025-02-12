# Copyright (c) 2022, Voltron Data.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import multiprocessing
import os
import pathlib
import platform

debug = True

default_repo_file = (
    "https://raw.githubusercontent.com/conbench/datalogistik/main/repo.json"
)
if platform.system() == "Windows":
    default_cache_location = os.path.join(
        os.getenv("LOCALAPPDATA"), "datalogistik_cache"
    )
else:  # Unix (Linux or Mac)
    default_cache_location = os.path.join(os.getenv("HOME"), ".datalogistik_cache")
metadata_filename = "datalogistik_metadata.ini"
supported_formats = ["parquet", "csv", "arrow", "ndjson", "tpc-raw"]
hashing_chunk_size = 16384


def get_cache_location():
    return pathlib.Path(os.getenv("DATALOGISTIK_CACHE", default_cache_location))


def get_repo_file_path():
    return os.getenv("DATALOGISTIK_REPO", default_repo_file)


def get_gen_location():
    return os.getenv("DATALOGISTIK_GEN", None)


def get_max_cpu_count():
    return int(os.getenv("DATALOGISTIK_MAX_THREADS", 0))


def get_thread_count():
    if get_max_cpu_count() != 0:
        return get_max_cpu_count()
    else:
        return max(1, multiprocessing.cpu_count())
