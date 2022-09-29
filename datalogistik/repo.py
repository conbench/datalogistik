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

import json

import urllib3

from . import config, tpc_info
from .dataset import Dataset
from .log import log


def get_repo():
    repo_location = config.get_repo_file_path()
    if repo_location[0:4] == "http":
        log.debug(f"Fetching repo from {repo_location}")
        try:
            http = urllib3.PoolManager()
            r = http.request("GET", repo_location)
            repository_json = json.loads(r.data.decode("utf-8"))
        except Exception:
            log.error(f"Unable to download from '{repo_location}'")
            raise
    else:
        with open(repo_location) as f:
            log.debug(f"Using local repo at {repo_location}")
            repository_json = json.load(f)
    return [Dataset.from_json(one) for one in repository_json]


def search_repo(name, repo):
    if name in tpc_info.tpc_datasets:
        return []

    index = [x.name for x in repo]

    # No dataset is found
    # TODO: do we actually need this tpc exception here?
    if name not in index and name not in tpc_info.tpc_datasets:
        return None

    return repo[index.index(name)]
