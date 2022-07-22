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
#

import datetime
import json
import os
import pathlib
import shutil
import tempfile

import pyarrow as pa
from pyarrow import csv
from pyarrow import dataset as ds

from datalogistik import __version__
from datalogistik import util
from datalogistik import config


def test_version():
    assert __version__ == "0.1.0"


def test_write_metadata():
    dataset_info = {
        "name": "hypothetical_dataset",
        "format": "csv",
        "delim": "|",
        "scale_factor": 1,  # N.B. This should NOT be in the metadata because name != tpc-h
        "partitioning-nrows": 0,
    }
    expected_metadata = {
        "name": dataset_info["name"],
        "format": dataset_info["format"],
        "delim": "|",
        "partitioning-nrows": dataset_info["partitioning-nrows"],
        "local-creation-date": datetime.datetime.now()
        .astimezone()
        .strftime("%Y-%m-%dT%H:%M:%S%z"),
        "files": [
            {
                "file_path": "tmpfile",
                "file_size": 18,
                "md5": "891bcd3700619af5151bf95b836ff9b1",
            },
            {
                "file_path": os.path.join("tmp2", "tmpfile2"),
                "file_size": 20,
                "md5": "0ccc9b1a435e7d40a91ac7dd04c67fe8",
            },
        ],
    }
    with tempfile.TemporaryDirectory() as path:
        with open(os.path.join(path, "tmpfile"), "w") as tmpfile:
            tmpfile.write("test file contents")
        path2 = os.path.join(path, "tmp2")
        os.mkdir(path2)
        with open(os.path.join(path2, "tmpfile2"), "w") as tmpfile2:
            tmpfile2.write("test file 2 contents")
        util.write_metadata(dataset_info, path)
        metadata_file_path = os.path.join(path, config.metadata_filename)
        written_metadata = json.load(open(metadata_file_path))
        assert written_metadata == expected_metadata


# TODO: test parquet->csv, partitioning conversion
def test_convert_dataset_csv_to_parquet():
    path = pathlib.Path("./test_csv/csv/0/")
    test_filename = "convtest"
    test_csv_file = test_filename + ".csv"
    test_parquet_file = test_filename + ".parquet"
    test_csv_file_path = pathlib.Path(path, test_csv_file)
    path.mkdir(parents=True, exist_ok=True)
    dataset_info = {
        "name": "test_csv",
        "url": "convtest.csv",
        "format": "csv",
        "partitioning-nrows": 0,
    }
    pydict = {"int": [1, 2], "str": ["a", "b"]}
    orig_table = pa.Table.from_pydict(pydict)
    print(orig_table.schema)
    csv.write_csv(orig_table, test_csv_file_path)
    util.write_metadata(dataset_info, path)
    written_table = csv.read_csv(test_csv_file_path)
    print(written_table.schema)
    assert written_table == orig_table
    converted_path = util.convert_dataset(
        ".", dataset_info, None, "csv", "parquet", 0, 0
    )
    test_parquet_file_path = pathlib.Path(converted_path, test_parquet_file)
    converted_table = ds.dataset(test_parquet_file_path).to_table()
    print(converted_table.schema)
    assert converted_table == orig_table
    (test_csv_file_path).unlink()
    (test_parquet_file_path / "part-0.parquet").unlink()
    (path / config.metadata_filename).unlink()
    (converted_path / config.metadata_filename).unlink()
    shutil.rmtree("./test_csv", ignore_errors=True)
