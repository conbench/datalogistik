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

import abc
import glob
import json
import os
import pathlib
from collections import OrderedDict
from typing import Optional

from pyarrow import dataset as pads

from . import config
from .log import log
from .util import get_csv_dataset, get_raw_tpc_dataset


class Dataset(abc.ABC):
    """A class that references a dataset.

    Parameters
    ----------
    name
        the name of the dataset
    metadata_file
        path to metadata file to construct
    format
        format of the dataset
    compression
        compression of the dataset
    paritioning
        partitioning (not currently exposed)
    rel_path
        the relative path to the dataset (either a file or a directory)
    """

    # We might want to make subclasses for each format (or possibly multi-inherit for each property) but that seems likely to be overkill, so for now using this one class for all datasets

    # TODO: are these really needed here if we need = None below?
    name: Optional[str] = None
    metadata_file: Optional[pathlib.Path] = None
    # TODO: literal type?
    format: Optional[str] = None
    compression: Optional[str] = None
    # partitioning is not fully implemented and should not be exposed to the user
    partitioning: Optional[int] = None
    rel_path: Optional[pathlib.Path] = None

    def __init__(
        self, name: Optional[str] = None, metadata_file: Optional[pathlib.Path] = None
    ):
        if metadata_file:
            self.metadata_file = pathlib.Path(metadata_file)
            with open(metadata_file) as f:
                dataset_metadata = json.load(f, object_pairs_hook=OrderedDict)
                self.name = dataset_metadata.get("name")
                self.format = dataset_metadata.get("format")
                self.compression = dataset_metadata.get("compression")
                self.partitioning = dataset_metadata.get("partitioning")
                self.rel_path = dataset_metadata.get("rel-path")
        else:
            self.name = name

    # For all datasets with this name in the cache, return a list of them
    def list_variants(self):
        # TODO: factor this out into a find helper? Then we can surface that | use that to find all variants extant?
        local_cache_location = config.get_cache_location()

        log.debug(f"Checking local cache at {local_cache_location}")

        metadata_files = glob.glob(
            os.path.join(
                local_cache_location, "**", self.name, "**", config.metadata_filename
            ),
            recursive=True,
        )

        return [Dataset(metadata_file=ds) for ds in metadata_files]

    def get_location(self):
        # The data must always a sibling of the metadata
        parent_dir = self.metadata_file.parent

        # TODO: check that this file actually exists?

        # TODO: allow for datasets with no extension
        # TODO: allow for compression extension bits (probably need to abstract into a name generator)
        data_path = os.path.join(parent_dir, self.name + os.extsep + self.format)

        return data_path

    def get_dataset(self):
        # Defaults
        schema = None
        format = self.format
        if format == "parquet":
            dataset_read_format = pads.ParquetFileFormat()
        if format == "csv":
            dataset_read_format, schema = get_csv_dataset(
                # though self == dataset_info, so need to munge
                self.name,
                self,
            )
        if format == "tpc-raw":
            dataset_read_format, schema = get_raw_tpc_dataset(
                # though self == dataset_info, so need to munge
                self.name,
                self,
            )

        return pads.dataset(
            self.get_location(), schema=schema, format=dataset_read_format
        )
