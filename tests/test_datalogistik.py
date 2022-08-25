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

import datetime
import json
import os
import pathlib
import tempfile

import pyarrow as pa
from pyarrow import csv
from pyarrow import dataset as ds
from pyarrow import parquet as pq

from datalogistik import __version__, config, util


def test_version():
    assert __version__ == "0.1.0"


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

arrow_types_noargs = {
    pa.null,
    pa.bool_,
    pa.int8,
    pa.int16,
    pa.int32,
    pa.int64,
    pa.uint8,
    pa.uint16,
    pa.uint32,
    pa.uint64,
    pa.float16,
    pa.float32,
    pa.float64,
    pa.date32,
    pa.date64,
    pa.month_day_nano_interval,
    pa.string,
    pa.utf8,
    pa.large_binary,
    pa.large_string,
    pa.large_utf8,
}
arrow_types_args = {
    (pa.time32("ms"), """"name": "time32", "args": {"unit": ms}"""),
    (pa.time64("us"), """"name": "time64", "args": {"unit": us}"""),
    (pa.timestamp("us"), """"name": "timestamp", "args": {"unit": us}"""),
    (pa.duration("us"), """"name": "binary", "args": {"unit": "us"}"""),
    (pa.binary(10), """"name": "binary", "args": {"size": 10}"""),
    (
        pa.decimal128(7, 3),
        """"name": "decimal", "args": {"precision": 7, "index": 3}""",
    ),
}
# These 2 should be equivalent
complete_schema_json = "{'a': 'null', 'b': 'bool', 'c': 'int8', 'd': 'int16', 'e': 'int32', 'f': 'int64', 'g': 'uint8', 'h': 'uint16', 'i': 'uint32', 'j': 'uint64', 'k': 'halffloat', 'l': 'float', 'm': 'double', 'n': 'date32[day]', 'o': 'date64[ms]', 'p': 'month_day_nano_interval', 'q': 'string', 'r': 'string', 's': 'large_binary', 't': 'large_string', 'u': 'large_string', 'v': 'time32[ms]', 'w': 'time64[us]', 'x': 'timestamp[s]', 'y': 'duration[ns]', 'z': 'fixed_size_binary[10]', 'argh': 'decimal128(7, 3)'}"
complete_schema = pa.schema(
    [
        pa.field("a", pa.null()),
        pa.field("b", pa.bool_()),
        pa.field("c", pa.int8()),
        pa.field("d", pa.int16()),
        pa.field("e", pa.int32()),
        pa.field("f", pa.int64()),
        pa.field("g", pa.uint8()),
        pa.field("h", pa.uint16()),
        pa.field("i", pa.uint32()),
        pa.field("j", pa.uint64()),
        pa.field("k", pa.float16()),
        pa.field("l", pa.float32()),
        pa.field("m", pa.float64()),
        pa.field("n", pa.date32()),
        pa.field("o", pa.date64()),
        pa.field("p", pa.month_day_nano_interval()),
        pa.field("q", pa.string()),
        pa.field("r", pa.utf8()),
        pa.field("s", pa.large_binary()),
        pa.field("t", pa.large_string()),
        pa.field("u", pa.large_utf8()),
        # types with arguments
        pa.field("v", pa.time32("ms")),
        pa.field("w", pa.time64("us")),
        pa.field("x", pa.timestamp("s")),
        pa.field("y", pa.duration("ns")),
        pa.field("z", pa.binary(10)),
        pa.field("argh", pa.decimal128(7, 3)),
    ]
)


def create_test_dataset(path):
    with open(os.path.join(path, "tmpfile"), "w") as tmpfile:
        tmpfile.write("test file contents")
        path2 = os.path.join(path, "tmp2")
        os.mkdir(path2)
        with open(os.path.join(path2, "tmpfile2"), "w") as tmpfile2:
            tmpfile2.write("test file 2 contents")
    util.write_metadata(dataset_info, path)


def test_write_metadata():
    with tempfile.TemporaryDirectory() as path:
        create_test_dataset(path)
        metadata_file_path = os.path.join(path, config.metadata_filename)
        with open(metadata_file_path) as f:
            written_metadata = json.load(f)
            assert written_metadata == expected_metadata


def test_validate():
    cache_root = config.get_cache_location()
    path = pathlib.Path(cache_root, "test_validate/csv/partitioning_0/")
    path.mkdir(parents=True, exist_ok=True)
    create_test_dataset(path)
    assert util.validate(path) is True
    metadata_file_path = pathlib.Path(path, config.metadata_filename)
    with open(metadata_file_path) as f:
        written_metadata = json.load(f)
    file_listing = written_metadata["files"]
    assert util.validate_files(path, file_listing) is True
    # Now change an md5sum in the metadata and check if the validation fails:
    file_listing[0]["md5"] = "00000000000000000000000000000000"
    assert util.validate_files(path, file_listing) is False

    # now test --validate and --prune-invalid
    util.validate_cache(True)  # this should not delete the entry
    assert metadata_file_path.exists()

    written_metadata["files"] = file_listing
    json_string = json.dumps(written_metadata)
    with open(metadata_file_path, "w") as f:
        f.write(json_string)
    util.validate_cache(False)  # this should not delete the entry, only report
    assert metadata_file_path.exists()
    util.validate_cache(True)  # this should delete the entry
    assert not metadata_file_path.exists()


# TODO: test partitioning conversion
def test_convert_dataset_csv_to_parquet():
    cache_root = config.get_cache_location()
    path = pathlib.Path(cache_root, "test_csv/csv/partitioning_0/")
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
    converted_path = util.convert_dataset(dataset_info, None, "csv", "parquet", 0, 0)
    test_parquet_file_path = pathlib.Path(converted_path, test_parquet_file)
    converted_table = ds.dataset(test_parquet_file_path).to_table()
    print(converted_table.schema)
    assert converted_table == orig_table
    util.prune_cache_entry("test_csv")


def test_convert_dataset_parquet_to_csv():
    cache_root = config.get_cache_location()
    path = pathlib.Path(cache_root, "test_parquet/parquet/partitioning_0/")
    test_filename = "convtest"
    test_csv_file = test_filename + ".csv"
    test_parquet_file = test_filename + ".parquet"
    test_parquet_file_path = pathlib.Path(path, test_parquet_file)
    path.mkdir(parents=True, exist_ok=True)
    dataset_info = {
        "name": "test_parquet",
        "url": "convtest.parquet",
        "format": "parquet",
        "partitioning-nrows": 0,
    }
    pydict = {"int": [1, 2], "str": ["a", "b"]}
    orig_table = pa.Table.from_pydict(pydict)
    print(orig_table.schema)
    pq.write_table(orig_table, test_parquet_file_path)
    util.write_metadata(dataset_info, path)
    written_table = pq.read_table(test_parquet_file_path)
    print(written_table.schema)
    assert written_table == orig_table
    converted_path = util.convert_dataset(dataset_info, None, "parquet", "csv", 0, 0)
    test_csv_file_path = pathlib.Path(converted_path, test_csv_file)
    converted_table = ds.dataset(
        test_csv_file_path, format=ds.CsvFileFormat()
    ).to_table()
    print(converted_table.schema)
    assert converted_table == orig_table
    util.prune_cache_entry("test_parquet")


def test_arrow_type_function_lookup():
    for func in arrow_types_noargs:
        assert func == util.arrow_type_function_lookup(func.__name__)


def test_arrow_type_from_json():
    for func in arrow_types_noargs:
        assert func() == util.arrow_type_from_json(func.__name__)


def test_get_arrow_schema():
    expected_arrow_schema = pa.schema(
        [
            ("a", pa.string()),
            ("b", pa.int64()),
            ("c", pa.float64()),
            ("d", pa.timestamp(unit="ms")),
        ]
    )
    json_schema = """{
        "a" : "string",
        "b" : "int64",
        "c" : {"name": "float64"},
        "d" : {"name": "timestamp", "arguments": {"unit": "ms"}}
        }"""
    arrow_schema = util.get_arrow_schema(json.loads(json_schema))
    assert arrow_schema == expected_arrow_schema


def test_schema_to_dict():
    assert str(util.schema_to_dict(complete_schema)) == complete_schema_json
