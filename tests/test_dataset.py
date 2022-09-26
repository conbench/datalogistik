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
import pathlib
import sys

import pytest
from pyarrow import dataset as pyarrowdataset

from datalogistik import config, dataset_search
from datalogistik.dataset import Dataset
from datalogistik.table import Table

simple_parquet_ds = Dataset.from_json(
    metadata="./tests/fixtures/test_cache/chi_traffic_sample/a1fa1fa/datalogistik_metadata.ini"
)
simple_csv_ds = Dataset.from_json(
    metadata="./tests/fixtures/test_cache/chi_traffic_sample/babb1e5/datalogistik_metadata.ini"
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
    assert set([ds.format for ds in ds_list]) == set(["csv", "parquet"])


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


def test_get_table_filename():
    assert (
        simple_parquet_ds.get_table_filename(simple_parquet_ds.tables[0])
        == "chi_traffic_sample.parquet"
    )

    # And we do the right thing with gzip, too
    assert (
        multi_table_ds.get_table_filename(multi_table_ds.tables[1])
        == "chi_traffic_sample.csv.gz"
    )


def test_ensure_table_loc():
    assert simple_parquet_ds.ensure_table_loc() == pathlib.Path(
        "tests/fixtures/test_cache/chi_traffic_sample/a1fa1fa/chi_traffic_sample.parquet"
    )
    assert multi_file_ds.ensure_table_loc() == pathlib.Path(
        "tests/fixtures/test_cache/taxi_2013/face7ed/taxi_2013"
    )

    assert simple_csv_ds.ensure_table_loc() == pathlib.Path(
        "tests/fixtures/test_cache/chi_traffic_sample/babb1e5/chi_traffic_sample.csv"
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
def test_get_table_dataset(test_dataset):
    arrow_ds = test_dataset.get_table_dataset(0)
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

    monkeypatch.setattr("datalogistik.util.download_file", _fake_download)
    ds_variant_not_available = Dataset(
        name="chi_traffic_2020_Q1", format="csv", compression="gzip"
    )
    ds_variant_not_available.download


def test_to_json():
    json_string = simple_parquet_ds.to_json()
    assert '"name": "chi_traffic_sample"' in json_string


@pytest.mark.skipif(sys.platform == "win32", reason="windows errors on the cleanup")
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
        tables=[Table(table="penguins", files=[{"file_path": "penguins.parquet"}])],
    )
    # We would use ensure_dataset in download, so use it here too
    penguins.ensure_dataset_loc("raw")
    penguins.write_metadata()

    metadata_out = pathlib.Path(
        "tests/fixtures/test_cache/penguins/raw/", config.metadata_filename
    )
    # now there is
    assert metadata_out.exists()

    # and we can read it in!
    read_dataset = Dataset.from_json(metadata_out)
    assert read_dataset.name == "penguins"

    # cleanup all files that weren't there to start with
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


def test_post_init():
    # We get defautl scale_factor=1
    ds = Dataset(name="tpc-h")
    assert ds.scale_factor == 1

    # uncompressed all uses None
    ds = Dataset(name="posty", compression="uncompressed")
    assert ds.compression is None

    # gzip and gz are both gzip
    ds = Dataset(name="posty", compression="gz")
    assert ds.compression == "gzip"


def test_output_result():
    expected = json.dumps(
        {
            "name": "chi_traffic_sample",
            "format": "parquet",
            "tables": {
                "chi_traffic_sample": {
                    "path": str(
                        pathlib.Path(
                            "tests",
                            "fixtures",
                            "test_cache",
                            "chi_traffic_sample",
                            "a1fa1fa",
                            "chi_traffic_sample.parquet",
                        )
                    ),
                    "dim": [],
                }
            },
        }
    )

    assert simple_parquet_ds.output_result() == expected

    expected = json.dumps(
        {
            "name": "chi_taxi",
            "format": "csv",
            "tables": {
                # This table is multi-file, so just the folder is passed
                "taxi_2013": {
                    "path": str(
                        pathlib.Path(
                            "tests",
                            "fixtures",
                            "test_cache",
                            "chi_taxi",
                            "dabb1e5",
                            "taxi_2013",
                        )
                    ),
                    "dim": [],
                },
                # This table is a single file, so we have the extension
                "chi_traffic_sample": {
                    "path": str(
                        pathlib.Path(
                            "tests",
                            "fixtures",
                            "test_cache",
                            "chi_taxi",
                            "dabb1e5",
                            "chi_traffic_sample.csv.gz",
                        )
                    ),
                    "dim": [],
                },
            },
        }
    )
    assert multi_table_ds.output_result() == expected


# dataset_search tests


def test_find_dataset():
    ds_to_find = Dataset(name="chi_traffic_sample", format="parquet")
    assert simple_parquet_ds == dataset_search.find_exact_dataset(ds_to_find)

    # but if there's no exact match, we get None
    ds_variant_not_found = Dataset(
        name="chi_traffic_sample", format="csv", compression="gzip"
    )
    assert dataset_search.find_exact_dataset(ds_variant_not_found) is None


# def test_find_close_dataset():
#     ds_variant_not_found = Dataset(
#         name="chi_traffic_sample", format="csv", compression="gzip"
#     )
#     close = find_close_dataset(ds_variant_not_found)
#     # We prefer Parquet if we have it
#     assert close.format == "parquet"
#     assert simple_parquet_ds == close

#     ds_variant_not_found = Dataset(name="nyctaxi_sample", format="parquet")
#     close = find_close_dataset(ds_variant_not_found)
#     # But can fall back
#     assert close.format == "csv"


def test_find_close_dataset_sf_mismatch(monkeypatch):
    # Mock the generation, cause all we care about here is that that would be called
    good_return = Dataset(name="tpc-h", scale_factor=10, format="tpc-raw")

    def _fake_generate(dataset):
        return good_return

    monkeypatch.setattr("datalogistik.util.generate_dataset", _fake_generate)

    # but some properties don't constitute a match:
    ds_diff_scale_factor = Dataset(name="tpc-h", scale_factor=10)
    output = dataset_search.find_or_instantiate_close_dataset(ds_diff_scale_factor)

    assert output is good_return


def test_get_csv_dataset_spec():
    ds = Dataset(
        name="tester",
        format="csv",
        extra_nulls=["FANCY_NULL"],
        tables=[Table(table="foo")],
    )
    spec, schema = ds.get_csv_dataset_spec(ds.tables[0])
    # it's a pyarrow quirk that the convert_options are under `default_fragment_scan_options`
    assert (
        "FANCY_NULL" in spec.default_fragment_scan_options.convert_options.null_values
    )


def test_fill_in_defaults():
    ds = Dataset(name="fanniemae_sample")
    dataset_from_repo = Dataset(name="fanniemae_sample", format="csv", delim="|")

    ds.fill_in_defaults(dataset_from_repo)
    assert ds.format == "csv"
    assert ds.delim == "|"

    # but we don't over-write if an attribute is given, and we never over-write compression (cause that turns into some weird circumstances)
    ds = Dataset(name="fanniemae_sample", format="parquet")
    dataset_from_repo = Dataset(
        name="fanniemae_sample", format="csv", delim="|", compression="gz"
    )

    ds.fill_in_defaults(dataset_from_repo)
    assert ds.format == "parquet"  # NB: not csv
    assert ds.compression is None


def test_get_dataset_with_schema():
    # TODO: can we alter the schema on the fly like this?
    # https://github.com/conbench/datalogistik/blob/027169a4194ba2eb27ff37889ad7e541bb4b4036/tests/test_datalogistik.py#L332-L358
    pass
