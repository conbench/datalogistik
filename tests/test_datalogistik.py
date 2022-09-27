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
import decimal
import json
import os
import pathlib
import random
import shutil
import string
import sys

import pyarrow as pa
import pytest

from datalogistik import __version__, cli, datalogistik, dataset, util


@pytest.fixture(autouse=True)
def mock_settings_env_vars(monkeypatch):
    monkeypatch.setenv("DATALOGISTIK_CACHE", "./tests/fixtures/test_cache")


def test_version():
    assert __version__ == "0.1.0"


def clean(path):
    util.prune_cache_entry(path)


test_dataset_info = {
    "name": "hypothetical_dataset",
    "format": "csv",
    "delim": "|",
    "scale_factor": 1,  # N.B. This should NOT be in the metadata because name != tpc-h
    "partitioning-nrows": 0,
}
expected_metadata = {
    "name": test_dataset_info["name"],
    "format": test_dataset_info["format"],
    "delim": "|",
    "partitioning-nrows": test_dataset_info["partitioning-nrows"],
    "local-creation-date": datetime.datetime.now()
    .astimezone()
    .strftime("%Y-%m-%dT%H:%M:%S%z"),
    "files": [
        {
            "rel_path": "testfile",
            "file_size": 18,
            "md5": "891bcd3700619af5151bf95b836ff9b1",
        },
        {
            "rel_path": os.path.join("tmp2", "testfile2"),
            "file_size": 20,
            "md5": "0ccc9b1a435e7d40a91ac7dd04c67fe8",
        },
    ],
}


def create_test_dataset(path):
    with open(os.path.join(path, "testfile"), "w") as testfile:
        testfile.write("test file contents")
        path2 = os.path.join(path, "tmp2")
        os.mkdir(path2)
        with open(os.path.join(path2, "testfile2"), "w") as testfile2:
            testfile2.write("test file 2 contents")
    util.write_metadata(test_dataset_info, path)


arrow_types_noarguments = {
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
arrow_types_arguments = {
    (pa.time32("ms"), """{"type_name": "time32", "arguments": "ms"}"""),
    (pa.time64("us"), """{"type_name": "time64", "arguments": "us"}"""),
    (pa.timestamp("us"), """{"type_name": "timestamp", "arguments": "us"}"""),
    (pa.duration("us"), """{"type_name": "duration", "arguments": "us"}"""),
    (pa.binary(10), """{"type_name": "binary", "arguments": {"length": 10}}"""),
    (
        pa.decimal128(7, 3),
        """{"type_name": "decimal", "arguments": {"precision": 7, "scale": 3}}""",
    ),
    # same type, but with the arguments as a list
    (
        pa.decimal128(7, 3),
        """{"type_name": "decimal", "arguments": [7, 3]}""",
    ),
}
# These schema's should be equivalent
complete_csv_schema_json_input = """{
    "a": "null",
    "b": "bool",
    "c": "int8",
    "d": "int16",
    "e": "int32",
    "f": "int64",
    "g": "uint8",
    "h": "uint16",
    "i": "uint32",
    "j": "uint64",
    "l": "float",
    "m": "double",
    "n": "date32",
    "o": "date64",
    "q": "string",
    "r": "string",
    "t": "large_string",
    "u": "large_string",
    "v": {"type_name": "time32", "arguments": "ms"},
    "w": {"type_name": "time64", "arguments": "us"},
    "x": {"type_name": "timestamp", "arguments": {"unit": "ms"}}
    }"""
complete_csv_schema_json_output = (
    "{'a': 'null', 'b': 'bool', 'c': 'int8', 'd': 'int16', 'e': 'int32', 'f': 'int64', "
    "'g': 'uint8', 'h': 'uint16', 'i': 'uint32', 'j': 'uint64', 'l': 'float', 'm': "
    "'double', 'n': 'date32[day]', 'o': 'date64[ms]', 'q': 'string', 'r': 'string', "
    "'t': 'large_string', 'u': 'large_string', 'v': 'time32[ms]', 'w': 'time64[us]', "
    "'x': 'timestamp[ms]'}"
)
complete_csv_schema = pa.schema(
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
        # pa.field("k", pa.float16()), # not supported by parquet and csv
        pa.field("l", pa.float32()),
        pa.field("m", pa.float64()),
        pa.field("n", pa.date32()),
        pa.field("o", pa.date64()),
        #  pa.field("p", pa.month_day_nano_interval()), # not supported by parquet and csv
        pa.field("q", pa.string()),
        pa.field("r", pa.utf8()),
        # pa.field("s", pa.large_binary()), # not supported by csv
        pa.field("t", pa.large_string()),
        pa.field("u", pa.large_utf8()),
        # types with arguments
        pa.field("v", pa.time32("ms")),
        pa.field("w", pa.time64("us")),
        pa.field("x", pa.timestamp("ms")),
        # pa.field("y", pa.duration("ns")), # not supported by parquet and csv
        # pa.field("z", pa.binary(10)), # not supported by csv
        # pa.field("argh", pa.decimal128(7, 3)),
    ]
)
complete_parquet_schema = pa.schema(
    [
        pa.field("a", pa.null()),
        pa.field("b", pa.bool_()),
        pa.field("c", pa.int8()),
        pa.field("d", pa.int16()),
        pa.field("e", pa.int32()),
        pa.field("f", pa.int64()),
        pa.field("g", pa.uint8()),
        pa.field("h", pa.uint16()),
        # pa.field("i", pa.uint32()), # not supported by parquet
        pa.field("j", pa.uint64()),
        # pa.field("k", pa.float16()), # not supported by parquet and csv
        pa.field("l", pa.float32()),
        pa.field("m", pa.float64()),
        pa.field("n", pa.date32()),
        # pa.field("o", pa.date64()), # not supported by parquet
        # pa.field("p", pa.month_day_nano_interval()), # not supported by parquet and csv
        pa.field("q", pa.string()),
        pa.field("r", pa.utf8()),
        pa.field("s", pa.large_binary()),
        pa.field("t", pa.large_string()),
        pa.field("u", pa.large_utf8()),
        # types with arguments
        pa.field("v", pa.time32("ms")),
        pa.field("w", pa.time64("us")),
        pa.field("x", pa.timestamp("us")),
        # pa.field("y", pa.duration("s")), # not supported by parquet and csv
        pa.field("z", pa.binary(10)),
        pa.field("argh", pa.decimal128(7, 3)),
    ]
)


def generate_random_string(length):
    random_string = ""
    for _ in range(length):
        random_string += random.choice(string.ascii_letters)
    return random_string


def generate_complete_schema_data(num_rows, format):
    k = num_rows
    data = {
        "a": pa.nulls(k),
        "b": random.choices([True, False], k=k),
        "c": [random.randint(-(2**8 / 2), 2**8 / 2 - 1) for _ in range(k)],
        "d": [random.randint(-(2**16 / 2), 2**16 / 2 - 1) for _ in range(k)],
        "e": [random.randint(-(2**32 / 2), 2**32 / 2 - 1) for _ in range(k)],
        "f": [random.randint(-(2**64 / 2), 2**64 / 2 - 1) for _ in range(k)],
        "g": [random.randint(0, 2**8 - 1) for _ in range(k)],
        "h": [random.randint(0, 2**16 - 1) for _ in range(k)],
        "j": [random.randint(0, 2**64 - 1) for _ in range(k)],
        # "k": [np.float16(random.random()) for _ in range(k)],
        "l": [random.random() for _ in range(k)],
        "m": [random.random() for _ in range(k)],
        "n": [
            (
                datetime.datetime(
                    random.randint(1970, 2270),
                    random.randint(1, 12),
                    random.randint(1, 28),
                )
                - datetime.datetime(1970, 1, 1)
            )
            // datetime.timedelta(days=1)
            for _ in range(k)
        ],
        # "p": [pa.MonthDayNano([random.randint(1,12),random.randint(1,28),random.randint(1,999) * 1000]) for _ in range(k)],
        "q": [generate_random_string(random.randint(1, 8)) for _ in range(k)],
        "r": [generate_random_string(random.randint(1, 8)) for _ in range(k)],
        "t": [generate_random_string(random.randint(1, 8)) for _ in range(k)],
        "u": [generate_random_string(random.randint(1, 8)) for _ in range(k)],
        # types with arguments
        "v": [
            datetime.timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )
            // datetime.timedelta(milliseconds=1)
            for _ in range(k)
        ],
        "w": [
            datetime.timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )
            // datetime.timedelta(microseconds=1)
            for _ in range(k)
        ],
        "x": [
            datetime.timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )
            // datetime.timedelta(milliseconds=1)
            for _ in range(k)
        ],
        # "y": [random.randint(0, 10e6) for _ in range(k)],
    }
    if format == "csv":
        data["i"] = [random.randint(0, 2**32 - 1) for _ in range(k)]
        data["o"] = [
            datetime.datetime(
                random.randint(1970, 2270),
                random.randint(1, 12),
                random.randint(1, 28),
                tzinfo=datetime.timezone.utc,
            ).timestamp()
            * 1000
            for _ in range(k)
        ]
    elif format == "parquet":
        data["s"] = [random.randbytes(random.randint(1, 64)) for _ in range(k)]
        data["z"] = [random.randbytes(10) for _ in range(k)]
        data["argh"] = [
            decimal.Decimal(f"{random.randint(0, 9999)}.{random.randint(0,999)}")
            for _ in range(k)
        ]
    else:
        raise (f"unsupport format {format}")
    return data


def test_arrow_type_function_lookup():
    for func in arrow_types_noarguments:
        assert func == util.arrow_type_function_lookup(func.__name__)


def test_arrow_type_from_json():
    for func in arrow_types_noarguments:
        assert func() == util.arrow_type_from_json(func.__name__)
    for (pa_type, json_type) in arrow_types_arguments:
        assert pa_type == util.arrow_type_from_json(json.loads(json_type))


def test_get_arrow_schema():
    parsed_schema = util.get_arrow_schema(json.loads(complete_csv_schema_json_input))
    assert parsed_schema == complete_csv_schema


def test_schema_to_dict():
    assert (
        str(util.schema_to_dict(complete_csv_schema)) == complete_csv_schema_json_output
    )


@pytest.mark.parametrize("comp_string", [None, "none", "NoNe", "uncompressed"])
def test_compress(comp_string):
    # These should all not compress/decompress since they are "the same"
    assert util.compress("uncompressed_file_path", "output_dir", comp_string) is None
    assert util.decompress("uncompressed_file_path", "output_dir", comp_string) is None


def test_instantiate(capsys):
    # This should be in the cache already, so no conversion needed
    exact_dataset = dataset.Dataset(name="fanniemae_sample", format="csv", delim="|")

    cli.instantiate(exact_dataset)

    captured = json.loads(capsys.readouterr().out)
    assert captured["name"] == "fanniemae_sample"
    assert captured["format"] == "csv"
    assert isinstance(captured["tables"], dict)


@pytest.mark.skipif(sys.platform == "win32", reason="windows errors on the cleanup")
def test_instantiate_with_convert(capsys):
    # the directory penguins has data _as if_ it were downloaded from a repo for the purposes of testing metadata writing
    test_dir_path = "tests/fixtures/test_cache/fanniemae_sample"

    # this should be only `penguins.parquet`
    start_files = os.listdir(test_dir_path)
    try:

        # This should be in the cache, but needs to be converted
        close_dataset = dataset.Dataset(name="fanniemae_sample", format="parquet")

        cli.instantiate(close_dataset)

        captured = json.loads(capsys.readouterr().out)
        assert captured["name"] == "fanniemae_sample"
        assert captured["format"] == "parquet"
        assert isinstance(captured["tables"], dict)

    finally:
        # cleanup all files that weren't there to start with
        for file in set(os.listdir(test_dir_path)) - set(start_files):
            shutil.rmtree(pathlib.Path(test_dir_path, file))


# Integration-style tests
def test_main(capsys):
    with pytest.raises(SystemExit) as e:
        sys.argv = [
            "datalogistik",
            "get",
            "-d",
            "fanniemae_sample",
            "-f",
            "csv",
        ]
        datalogistik.main()
        assert e.type == SystemExit
        assert e.value.code == 0

    captured = json.loads(capsys.readouterr().out)
    assert captured["name"] == "fanniemae_sample"
    assert captured["format"] == "csv"
    assert isinstance(captured["tables"], dict)


@pytest.mark.skipif(sys.platform == "win32", reason="windows errors on the cleanup")
def test_main_with_convert(capsys):
    # the directory penguins has data _as if_ it were downloaded from a repo for the purposes of testing metadata writing
    test_dir_path = "tests/fixtures/test_cache/fanniemae_sample"

    # this should be only `penguins.parquet`
    start_files = os.listdir(test_dir_path)
    try:

        with pytest.raises(SystemExit) as e:
            sys.argv = [
                "datalogistik",
                "get",
                "-d",
                "fanniemae_sample",
                "-f",
                "parquet",
            ]
            datalogistik.main()
            assert e.type == SystemExit
            assert e.value.code == 0

        captured = json.loads(capsys.readouterr().out)
        assert captured["name"] == "fanniemae_sample"
        assert captured["format"] == "parquet"
        assert isinstance(captured["tables"], dict)

    finally:
        # cleanup all files that weren't there to start with
        for file in set(os.listdir(test_dir_path)) - set(start_files):
            shutil.rmtree(pathlib.Path(test_dir_path, file))
