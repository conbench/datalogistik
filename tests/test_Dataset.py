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
from unittest import TestCase, mock

import pytest
from pyarrow import dataset as pyarrowdataset

# TOOD: this seems like the wrong way to do this, should we move this up?
from datalogistik import Dataset

simple_parquet_ds = Dataset.Dataset(
    metadata_file="./tests/fixtures/.datalogistik_cache/chi_traffic_sample/a1fa1fa/datalogistik_metadata.ini"
)
multi_file_ds = Dataset.Dataset(
    metadata_file="./tests/fixtures/.datalogistik_cache/taxi_2013/face7ed/datalogistik_metadata.ini"
)


@pytest.fixture(autouse=True)
def mock_settings_env_vars():
    with mock.patch.dict(os.environ, {"DATALOGISTIK_CACHE": "./tests/fixtures/"}):
        yield


def test_create_Dataset_from_name():
    ds = Dataset.Dataset(name="new one")
    assert ds.name == "new one"


def test_create_Dataset_from_metadata_file():
    assert simple_parquet_ds.name == "chi_traffic_sample"
    assert simple_parquet_ds.format == "parquet"


def test_list_variants():
    ds_list = simple_parquet_ds.list_variants()
    assert len(ds_list) == 2

    # We have all and only chi_traffic_sample
    # TODO: is this actually how to use TestCase + assertCountEqual?
    TestCase().assertCountEqual(
        [ds.name for ds in ds_list], ["chi_traffic_sample", "chi_traffic_sample"]
    )

    # assert that the formats are what we expect (so we know we've read in the files)
    TestCase().assertCountEqual([ds.format for ds in ds_list], ["parquet", "csv"])


def test_get_location():
    # TODO: add unit tests
    pass


def test_get_dataset():
    arrow_ds = simple_parquet_ds.get_dataset()

    TestCase().assertIsInstance(arrow_ds, pyarrowdataset.Dataset)
