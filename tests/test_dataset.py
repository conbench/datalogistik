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

import copy
import json
import os
import pathlib

import pytest
from pyarrow import dataset as pyarrowdataset

from datalogistik import config
from datalogistik.dataset import Dataset
from datalogistik.dataset_search import find_close_dataset, find_exact_dataset
from datalogistik.table import Table

simple_parquet_ds = Dataset.from_json(
    metadata="./tests/fixtures/test_cache/chi_traffic_sample/a1fa1fa/datalogistik_metadata.ini"
)
multi_file_ds = Dataset.from_json(
    metadata="./tests/fixtures/test_cache/taxi_2013/face7ed/datalogistik_metadata.ini"
)
multi_table_ds = Dataset.from_json(
    metadata="./tests/fixtures/test_cache/chi_taxi/dabb1e5/datalogistik_metadata.ini"
)


@pytest.fixture(autouse=True)
def mock_settings_env_vars(monkeypatch):
    monkeypatch.setenv("DATALOGISTIK_CACHE", "./tests/fixtures/test_cache")


def test_create_Dataset_from_name():
    ds = Dataset(name="new one")
    assert ds.name == "new one"


def test_create_Dataset_from_metadata_file():
    assert simple_parquet_ds.name == "chi_traffic_sample"
    assert simple_parquet_ds.format == "parquet"


def test_list_variants():
    ds_list = simple_parquet_ds.list_variants()
    assert len(ds_list) == 2

    # We have all and only chi_traffic_sample
    assert [ds.name for ds in ds_list] == ["chi_traffic_sample", "chi_traffic_sample"]

    # assert that the formats are what we expect (so we know we've read in the files)
    assert [ds.format for ds in ds_list] == ["csv", "parquet"]


def test_ensure_dataset_loc():
    # A file already in the cache
    assert simple_parquet_ds.ensure_dataset_loc() == pathlib.Path(
        "tests/fixtures/test_cache/chi_traffic_sample/a1fa1fa"
    )

    # a dataset that's not yet in the cache
    new_ds = Dataset(name="new_dataset")
    assert new_ds.ensure_dataset_loc() == pathlib.Path(
        "tests/fixtures/test_cache/new_dataset/raw"
    )

    # a dataset that has an overridden path
    new_ds = Dataset(name="new_dataset", cache_location=pathlib.Path("foo/bar/baz"))
    assert new_ds.ensure_dataset_loc() == pathlib.Path("foo/bar/baz")
    # TODO: assert the dir is made?


def test_get_table_name():
    assert (
        simple_parquet_ds.get_table_name(simple_parquet_ds.tables[0])
        == "chi_traffic_sample.parquet"
    )

    # now fake a multi-file name:
    new_ds = copy.deepcopy(simple_parquet_ds)
    new_ds.tables[0].multi_file = True
    assert new_ds.get_table_name(new_ds.tables[0]) == "chi_traffic_sample"


def test_ensure_table_loc():
    assert simple_parquet_ds.ensure_table_loc() == pathlib.Path(
        "tests/fixtures/test_cache/chi_traffic_sample/a1fa1fa/chi_traffic_sample.parquet"
    )
    assert multi_file_ds.ensure_table_loc() == pathlib.Path(
        "tests/fixtures/test_cache/taxi_2013/face7ed/taxi_2013"
    )


def test_get_one_table():
    tab1 = Table(table="one")
    tab2 = Table(table="two")
    ds = Dataset(name="tester", tables=[tab1, tab2])
    assert ds.get_one_table(tab1) == tab1
    assert ds.get_one_table([tab1]) == tab1
    assert ds.get_one_table(0) == tab1
    assert ds.get_one_table(1) == tab2
    assert ds.get_one_table("one") == tab1
    assert ds.get_one_table("two") == tab2
    with pytest.warns(UserWarning, match="This dataset has more than one table"):
        assert ds.get_one_table() == tab1


@pytest.mark.parametrize(
    "test_dataset", [simple_parquet_ds, multi_file_ds, multi_table_ds]
)
def test_get_table(test_dataset):
    arrow_ds = test_dataset.get_table(0)
    assert isinstance(arrow_ds, pyarrowdataset.Dataset)


def test_download_dataset(monkeypatch):
    def _fake_download(url, output_path):
        assert (
            url
            == "https://ursa-qa.s3.amazonaws.com/chitraffic/chi_traffic_2020_Q1.parquet"
        )
        assert output_path == pathlib.Path(
            "tests/fixtures/test_cache/chi_traffic_2020_Q1/raw/chi_traffic_2020_Q1.parquet"
        )

    monkeypatch.setattr("datalogistik.dataset.download_file", _fake_download)
    ds_variant_not_available = Dataset(
        name="chi_traffic_2020_Q1", format="csv", compression="gzip"
    )
    ds_variant_not_available.download


def test_to_json():
    json_string = simple_parquet_ds.to_json()
    assert '"name": "chi_traffic_sample"' in json_string


def test_write_metadata():
    # the directory penguins has data _as if_ it were downloaded from a repo for the purposes of testing metadata writing
    test_dir_path = "tests/fixtures/test_cache/penguins/raw"

    # this should be only `penguins.parquet`
    start_files = os.listdir(test_dir_path)

    # there is not a metadata file already
    assert not pathlib.Path(
        "tests/fixtures/test_cache/penguins/raw/", config.metadata_filename
    ).exists()

    # We will need to cleanup in this directory
    penguins = Dataset(
        name="penguins",
        format="parquet",
        tables=[Table(table="penguins", files=["penguins.parquet"])],
    )
    # We would use ensure_dataset in download, so use it here too
    penguins.ensure_dataset_loc("raw")
    penguins.write_metadata()

    # now there is
    assert pathlib.Path(
        "tests/fixtures/test_cache/penguins/raw/", config.metadata_filename
    ).exists()

    # cleanup all files that werent' there to start with
    for file in set(os.listdir(test_dir_path)) - set(start_files):
        pathlib.Path(test_dir_path, file).unlink()


def test_eq():
    ds1 = Dataset(name="one name")
    ds2 = Dataset(name="one name")
    assert ds1 == ds2

    ds1.compression = "snappy"
    ds2.compression = "snappy"
    assert ds1 == ds2

    # but we ignore irrelevant fields:
    ds1.homepage = "https://google.com"
    ds2.homepage = "https://yahoo.com"
    assert ds1 == ds2

    # and we can use different names for uncompressed
    ds1.compression = None
    ds2.compression = "none"
    assert ds1 == ds2

    ds1.compression = "uncompressed"
    ds2.compression = None
    assert ds1 == ds2


def test_output_result():
    expected = json.dumps(
        {
            "name": "chi_traffic_sample",
            "format": "parquet",
            "tables": {
                "chi_traffic_sample": "tests/fixtures/test_cache/chi_traffic_sample/a1fa1fa/chi_traffic_sample.parquet"
            },
        }
    )

    assert simple_parquet_ds.output_result() == expected

    expected = json.dumps(
        {
            "name": "chi_taxi",
            "format": "csv",
            "tables": {
                # This table is a multi file dataset, so just the folder is passed
                "taxi_2013": "tests/fixtures/test_cache/chi_taxi/dabb1e5/taxi_2013",
                # This table is a single file dataset, so we have the extension
                "chi_traffic_sample": "tests/fixtures/test_cache/chi_taxi/dabb1e5/chi_traffic_sample.csv",
            },
        }
    )
    assert multi_table_ds.output_result() == expected


# dataset_search tests


def test_find_dataset():
    ds_to_find = Dataset(name="chi_traffic_sample", format="parquet")
    assert simple_parquet_ds == find_exact_dataset(ds_to_find)

    # but if there's no exact match, we get None
    ds_variant_not_found = Dataset(
        name="chi_traffic_sample", format="csv", compression="gzip"
    )
    assert find_exact_dataset(ds_variant_not_found) is None


def test_find_close_dataset():
    ds_variant_not_found = Dataset(
        name="chi_traffic_sample", format="csv", compression="gzip"
    )
    close = find_close_dataset(ds_variant_not_found)
    # We prefer Parquet if we have it
    assert close.format == "parquet"
    assert simple_parquet_ds == close

    ds_variant_not_found = Dataset(name="nyctaxi_sample", format="parquet")
    close = find_close_dataset(ds_variant_not_found)
    # But can fall back
    assert close.format == "csv"


# def test_get_dataset_with_schema():
#     num_rows = 10
#     name = "test_complete_dataset"
#     clean(name)
#     path = util.create_cached_dataset_path(name, None, "csv", 0, None)
#     path.mkdir(parents=True)
#     test_file = path / "complete_data.csv"
#     data = generate_complete_schema_data(num_rows, "csv")
#     ref_table = pa.table(data, schema=complete_csv_schema)
#     wo = csv.WriteOptions(include_header=False)
#     csv.write_csv(ref_table, test_file, write_options=wo)
#     complete_dataset_info = {
#         "name": name,
#         "format": "csv",
#         "tables": [
#             {
#                 "table": "complete_data",
#                 "schema": json.loads(complete_csv_schema_json_input),
#             }
#         ],
#     }
#     util.write_metadata(complete_dataset_info, path)
#     dataset = util.get_dataset(test_file, complete_dataset_info)
#     read_table = dataset.to_table()

#     assert ref_table == read_table
#     clean(name)
