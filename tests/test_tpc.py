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
import os
import sys
import tempfile

import pytest

from datalogistik import datalogistik, tpc_validation

# TODO: TPC-DS tests & validation
# TODO: add more unittest for testing more parameters (sf, partitions, format)


@pytest.mark.parametrize("dataset_name", ["tpc-h"])
@pytest.mark.parametrize("format", ["parquet"])
def test_validate_tpc_generation(capsys, dataset_name, format):
    with tempfile.TemporaryDirectory() as tmpcachepath:
        os.environ["DATALOGISTIK_CACHE"] = tmpcachepath
        with pytest.raises(SystemExit) as e:
            sys.argv = [
                "test_tpc",
                "get",
                "-d",
                dataset_name,
                "-s",
                "0.001" if dataset_name == "tpc-h" else "1",
                "-f",
                format,
            ]
            datalogistik.main()
            assert e.type == SystemExit
            assert e.value.code == 0

        captured = capsys.readouterr().out
        dataset_result = json.loads(captured)
        dataset_name = dataset_result["name"]
        file_format = dataset_result["format"]
        paths_dict = dataset_result["tables"]
        assert tpc_validation.validate_tpc_dataset(
            dataset_name, paths_dict, file_format
        )
