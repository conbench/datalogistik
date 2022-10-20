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

import pytest

from datalogistik.dataset import Dataset
from datalogistik.repo import get_repo, search_repo


@pytest.fixture(autouse=True)
def mock_settings_env_vars(monkeypatch):
    monkeypatch.setenv("DATALOGISTIK_CACHE", "./tests/fixtures/datalogistik_cache")
    # Use local repo file in case we are making changes to it
    monkeypatch.setenv("DATALOGISTIK_REPO", "./repo.json")


def test_get_repo():
    repo_data = get_repo()
    assert isinstance(repo_data, list)
    assert isinstance(repo_data[0], Dataset)


def test_search_repo():
    repo_data = get_repo()
    fannie = search_repo("fanniemae_2016Q4", repo_data)
    assert isinstance(fannie, Dataset)

    assert search_repo("nonexistingdataset", repo_data) is None
