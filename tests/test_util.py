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

import os
from pathlib import Path

import pytest

from datalogistik.util import set_readonly


@pytest.fixture(scope="function")
def test_file(tmp_path_factory):
    path = tmp_path_factory.mktemp("data") / "file.ext"
    Path(path).write_text("")
    os.chmod(path, 0o777)
    return path


def test_set_readonly(test_file):
    new_file = test_file
    assert oct(os.stat(new_file).st_mode) == "0o100777"
    set_readonly(new_file)
    assert oct(os.stat(new_file).st_mode) == "0o100444"


def test_set_readonly_with_envvar(test_file, monkeypatch):
    monkeypatch.setenv("DATALOGISTIK_NO_PERMISSIONS_CHANGE", "True")
    new_file = test_file
    assert oct(os.stat(new_file).st_mode) == "0o100777"
    set_readonly(new_file)
    assert oct(os.stat(new_file).st_mode) == "0o100777"
