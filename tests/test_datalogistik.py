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
import re
import shutil
import string
import sys
import tempfile

import ndjson
import numpy as np
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.feather as feather  # arrow format
import pyarrow.json as pa_json
import pyarrow.parquet as pq
import pytest

from datalogistik import __version__, config, datalogistik, util
from datalogistik.dataset import Dataset


@pytest.fixture(autouse=True)
def mock_settings_env_vars(monkeypatch):
    monkeypatch.setenv("DATALOGISTIK_CACHE", "./tests/fixtures/test_cache")


def test_version():
    assert __version__ == "0.1.0"


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
complete_schema_json_input = """{
    "null": "null",
    "bool": "bool",
    "int8": "int8",
    "int16": "int16",
    "int32": "int32",
    "int64": "int64",
    "uint8": "uint8",
    "uint16": "uint16",
    "uint32": "uint32",
    "uint64": "uint64",
    "float16": "float16",
    "float32": "float",
    "float64": "double",
    "date32": "date32",
    "date64": "date64",
    "month_day_nano_interval": "month_day_nano_interval",
    "string": "string",
    "utf8": "string",
    "large_binary": "large_binary",
    "large_string": "large_string",
    "large_utf8": "large_string",
    "time32": {"type_name": "time32", "arguments": "ms"},
    "time64": {"type_name": "time64", "arguments": "us"},
    "timestamp": {"type_name": "timestamp", "arguments": {"unit": "us"}},
    "duration": {"type_name": "duration", "arguments": "us"},
    "binary": {"type_name": "binary", "arguments": {"length": 10}},
    "decimal": {"type_name": "decimal", "arguments": {"precision": 7, "scale": 3}}
}"""
complete_schema_json_output = (
    "{'null': 'null', 'bool': 'bool', 'int8': 'int8', 'int16': 'int16', "
    "'int32': 'int32', 'int64': 'int64', 'uint8': 'uint8', 'uint16': 'uint16', "
    "'uint32': 'uint32', 'uint64': 'uint64', 'float16': 'halffloat', 'float32': "
    "'float', 'float64': 'double', 'date32': 'date32[day]', 'date64': 'date64[ms]', "
    "'month_day_nano_interval': 'month_day_nano_interval', 'string': 'string', "
    "'utf8': 'string', 'large_binary': 'large_binary', 'large_string': 'large_string', "
    "'large_utf8': 'large_string', 'time32': 'time32[ms]', 'time64': 'time64[us]', "
    "'timestamp': 'timestamp[us]', 'duration': 'duration[us]', "
    "'binary': 'fixed_size_binary[10]', 'decimal': 'decimal128(7, 3)'}"
)
complete_schema = pa.schema(
    [
        pa.field("null", pa.null()),
        pa.field("bool", pa.bool_()),
        pa.field("int8", pa.int8()),
        pa.field("int16", pa.int16()),
        pa.field("int32", pa.int32()),
        pa.field("int64", pa.int64()),
        pa.field("uint8", pa.uint8()),
        pa.field("uint16", pa.uint16()),
        pa.field("uint32", pa.uint32()),  # not supported by parquet
        pa.field("uint64", pa.uint64()),
        pa.field("float16", pa.float16()),  # not supported by parquet, csv and ndjson
        pa.field("float32", pa.float32()),
        pa.field("float64", pa.float64()),
        pa.field("date32", pa.date32()),  # not supported by ndjson
        pa.field("date64", pa.date64()),  # not supported by parquet and ndjson
        pa.field(
            "month_day_nano_interval", pa.month_day_nano_interval()
        ),  # not supported by parquet, csv and ndjson
        pa.field("string", pa.string()),
        pa.field("utf8", pa.utf8()),
        pa.field("large_binary", pa.large_binary()),  # not supported by csv and ndjson
        pa.field("large_string", pa.large_string()),
        pa.field("large_utf8", pa.large_utf8()),
        # types with arguments
        pa.field("time32", pa.time32("ms")),  # not supported by ndjson
        pa.field("time64", pa.time64("us")),  # not supported by ndjson
        pa.field("timestamp", pa.timestamp("us")),  # not supported by ndjson
        pa.field(
            "duration", pa.duration("us")
        ),  # not supported by parquet, csv and ndjson
        pa.field("binary", pa.binary(10)),  # not supported by csv and ndjson
        pa.field("decimal", pa.decimal128(7, 3)),  # not supported by csv and ndjson
    ]
)


def generate_random_string(length):
    random_string = ""
    for _ in range(length):
        random_string += random.choice(string.ascii_letters)
    return random_string


def randbytes(length):
    return random.getrandbits(length * 8).to_bytes(length, "little")


# Generate random data according to the complete schemas above.
# Not all datatypes are supported by all formats.
# Note that if format is not set to one of the 3 supported formats
# (parquet, csv, arrow), the resulting data is supported by all 3.
def generate_complete_schema_data(num_rows):
    k = num_rows

    data = {
        "null": pa.nulls(k),
        "bool": random.choices([True, False], k=k),
        "int8": [random.randint(-(2**8 / 2), 2**8 / 2 - 1) for _ in range(k)],
        "int16": [random.randint(-(2**16 / 2), 2**16 / 2 - 1) for _ in range(k)],
        "int32": [random.randint(-(2**32 / 2), 2**32 / 2 - 1) for _ in range(k)],
        "int64": [random.randint(-(2**64 / 2), 2**64 / 2 - 1) for _ in range(k)],
        "uint8": [random.randint(0, 2**8 - 1) for _ in range(k)],
        "uint16": [random.randint(0, 2**16 - 1) for _ in range(k)],
        "uint32": [random.randint(0, 2**32 - 1) for _ in range(k)],
        "uint64": [random.randint(0, 2**64 - 1) for _ in range(k)],
        "float16": [np.float16(random.random()) for _ in range(k)],
        "float32": [random.random() for _ in range(k)],
        "float64": [random.random() for _ in range(k)],
        "date32": [
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
        "date64": [
            datetime.datetime(
                random.randint(1970, 2270),
                random.randint(1, 12),
                random.randint(1, 28),
                tzinfo=datetime.timezone.utc,
            ).timestamp()
            * 1000
            for _ in range(k)
        ],
        "month_day_nano_interval": [
            pa.MonthDayNano(
                [
                    random.randint(1, 12),
                    random.randint(1, 28),
                    random.randint(1, 999) * 1000,
                ]
            )
            for _ in range(k)
        ],
        "string": [generate_random_string(random.randint(1, 8)) for _ in range(k)],
        "utf8": [generate_random_string(random.randint(1, 8)) for _ in range(k)],
        "large_binary": [randbytes(random.randint(1, 64)) for _ in range(k)],
        "large_string": [
            generate_random_string(random.randint(1, 8)) for _ in range(k)
        ],
        "large_utf8": [generate_random_string(random.randint(1, 8)) for _ in range(k)],
        # types with arguments
        "time32": [
            datetime.timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )
            // datetime.timedelta(milliseconds=1)
            for _ in range(k)
        ],
        "time64": [
            datetime.timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )
            // datetime.timedelta(microseconds=1)
            for _ in range(k)
        ],
        "timestamp": [
            datetime.timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )
            // datetime.timedelta(microseconds=1)
            for _ in range(k)
        ],
        "duration": [random.randint(0, 10e6) for _ in range(k)],
        "binary": [randbytes(10) for _ in range(k)],
        "decimal": [
            decimal.Decimal(f"{random.randint(0, 9999)}.{random.randint(0,999)}")
            for _ in range(k)
        ],
    }
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
    parsed_schema = util.get_arrow_schema(json.loads(complete_schema_json_input))
    assert parsed_schema == complete_schema


def test_schema_to_dict():
    assert str(util.schema_to_dict(complete_schema)) == complete_schema_json_output


@pytest.mark.parametrize("comp_string", [None, "none", "NoNe", "uncompressed"])
def test_compress(comp_string):
    # These should all not compress/decompress since they are "the same"
    assert util.compress("uncompressed_file_path", "output_dir", comp_string) is None
    assert util.decompress("uncompressed_file_path", "output_dir", comp_string) is None


def remove_unsupported_types(data, source_format, dest_format):
    subset_schema = complete_schema
    subset_json_schema = complete_schema_json_input
    unsupported_columns = []
    if "csv" in [source_format, dest_format]:
        unsupported_columns.extend(
            [
                "float16",
                "month_day_nano_interval",
                "large_binary",
                "duration",
                "binary",
                "decimal",
            ]
        )
    if "parquet" in [source_format, dest_format]:
        unsupported_columns.extend(
            ["uint32", "float16", "date64", "month_day_nano_interval", "duration"]
        )
    if "ndjson" in [source_format, dest_format]:
        unsupported_columns.extend(
            [
                "float16",
                "date32",
                "date64",
                "month_day_nano_interval",
                "large_binary",
                "time32",
                "time64",
                "timestamp",
                "duration",
                "binary",
                "decimal",
            ]
        )

    # remove duplicates
    unsupported_columns = list(dict.fromkeys(unsupported_columns))
    for col in unsupported_columns:
        if data.get(col, None):
            del data[col]
        subset_schema = subset_schema.remove(subset_schema.get_field_index(col))
        regex = f'.*"{col}":.*\n'
        subset_json_schema = re.sub(regex, "", subset_json_schema, re.MULTILINE)

    # fix up the trailing comma in case we removed the last line
    subset_json_schema = re.sub(",\n}", "\n}", subset_json_schema, re.MULTILINE)

    return data, subset_schema, subset_json_schema


@pytest.mark.parametrize("source_format", ["csv", "parquet", "arrow", "ndjson"])
@pytest.mark.parametrize("dest_format", ["csv", "parquet", "arrow", "ndjson"])
def test_convert_parquet(monkeypatch, source_format, dest_format):
    name = "data_to_be_converted"
    file_name = name + "." + source_format
    data = generate_complete_schema_data(100)
    data, schema, schema_json = remove_unsupported_types(
        data, source_format, dest_format
    )

    orig_table = pa.table(data, schema=schema)
    with tempfile.TemporaryDirectory() as tmpdspath:
        monkeypatch.setenv("DATALOGISTIK_CACHE", tmpdspath)
        jpo = pa_json.ParseOptions(explicit_schema=schema)
        complete_dataset_info = {
            "name": name,
            "format": source_format,
            "tables": [
                {
                    "table": name,
                    "header_line": False,
                    "schema": json.loads(schema_json),
                }
            ],
        }
        rawdir = pathlib.Path(tmpdspath, name, "raw")
        rawdir.mkdir(parents=True)
        meta_file_path = rawdir / config.metadata_filename
        with open(meta_file_path, "w") as metafile:
            json.dump(complete_dataset_info, metafile)
        source_file = rawdir / file_name
        if source_format == "csv":
            wo = csv.WriteOptions(include_header=False)
            csv.write_csv(orig_table, source_file, write_options=wo)
        elif source_format == "parquet":
            pq.write_table(orig_table, source_file)
        elif source_format == "arrow":
            feather.write_feather(orig_table, source_file)
        elif source_format == "ndjson":
            with open(source_file, "w") as f:
                writer = ndjson.writer(f)
                for row in orig_table.to_pylist():
                    writer.writerow(row)
        dataset = Dataset.from_json_file(meta_file_path)
        if source_format == "ndjson":
            written_table = pa_json.read_json(
                dataset.ensure_table_loc(), parse_options=jpo
            )
        else:
            written_table = dataset.get_table_dataset().to_table()
        assert written_table == orig_table
        target_dataset = Dataset(name=name, format=dest_format)
        converted_dataset = dataset.convert(target_dataset)
        if dest_format == "ndjson":
            converted_table = pa_json.read_json(
                converted_dataset.ensure_table_loc(), parse_options=jpo
            )
        else:
            converted_table = converted_dataset.get_table_dataset().to_table()
        assert converted_table == orig_table


# Integration-style tests
@pytest.mark.skipif(sys.platform == "win32", reason="windows path seperator")
def test_main(capsys):
    # This should be in the cache already, so no conversion needed
    exact_dataset = Dataset(
        name="fanniemae_sample", format="csv", delim="|", compression="gzip"
    )

    with pytest.raises(SystemExit) as e:
        datalogistik.main(exact_dataset)
        assert e.type == SystemExit
        assert e.value.code == 0

    captured = json.loads(capsys.readouterr().out)
    assert captured["name"] == "fanniemae_sample"
    assert captured["format"] == "csv"
    assert isinstance(captured["tables"], dict)
    # this is the path from the fixtures, if this doesn't match, we've actualy converted and not just found the extant one
    assert (
        captured["tables"]["fanniemae_sample"]["path"]
        == "tests/fixtures/test_cache/fanniemae_sample/a77e575/fanniemae_sample.csv.gz"
    )


@pytest.mark.skipif(sys.platform == "win32", reason="windows errors on the cleanup")
def test_main_with_convert(capsys):
    # the directory penguins has data _as if_ it were downloaded from a repo for the purposes of testing metadata writing
    test_dir_path = "tests/fixtures/test_cache/fanniemae_sample"

    # this should be only `penguins.parquet`
    start_files = os.listdir(test_dir_path)
    try:

        # This should be in the cache, but needs to be converted
        close_dataset = Dataset(name="fanniemae_sample", format="parquet")

        with pytest.raises(SystemExit) as e:
            datalogistik.main(close_dataset)
            assert e.type == SystemExit
            assert e.value.code == 0

        captured = json.loads(capsys.readouterr().out)
        assert captured["name"] == "fanniemae_sample"
        assert captured["format"] == "parquet"
        assert isinstance(captured["tables"], dict)

    finally:
        # cleanup all files that weren't there to start with
        for file in set(os.listdir(test_dir_path)) - set(start_files):
            path = pathlib.Path(test_dir_path, file)
            util.set_readwrite_recurse(path)
            shutil.rmtree(path)
