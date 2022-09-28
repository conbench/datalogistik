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

import dataclasses
import datetime
import json
import os
import pathlib
import time
import warnings
from collections import OrderedDict
from dataclasses import asdict, dataclass, field, fields
from typing import List, Optional

import pyarrow as pa
from pyarrow import csv
from pyarrow import dataset as pads
from pyarrow import parquet as pq

from . import config, tpc_info, util
from .log import log
from .table import Table


@dataclass
class Dataset:
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
    """

    # We might want to make subclasses for each format (or possibly multi-inherit for each property) but that seems likely to be overkill, so for now using this one class for all datasets

    name: Optional[str] = None
    metadata_file: Optional[pathlib.Path] = None
    # TODO: literal type? for specific values
    format: Optional[str] = None
    compression: Optional[str] = None
    tables: Optional[List] = field(default_factory=list)
    local_creation_date: Optional[str] = None
    scale_factor: Optional[float] = None
    delim: Optional[str] = None
    metadata_file: Optional[pathlib.Path] = None
    url: Optional[str] = None
    homepage: Optional[str] = None
    # a list of strings that can be added when csv parsing to treat as if they were nulls
    extra_nulls: Optional[List] = field(default_factory=list)

    # To be filled in at run time only
    cache_location: Optional[pathlib.Path] = None
    dir_hash: Optional[str] = None

    # To be filled in programmatically when a dataset is created
    local_creation_date: Optional[str] = None

    def __post_init__(self):
        if self.format is not None and self.format not in config.supported_formats:
            msg = f"Unsupported format: {self.format}. Supported formats: {config.supported_formats}"
            log.error(msg)
            raise RuntimeError(msg)
        if self.scale_factor is None and self.name in tpc_info.tpc_datasets:
            self.scale_factor = 1.0

        # Use None as the true default for uncompressed
        # the first comparison is a bit redundant, but None.lower() fails
        if (
            self.compression is None
            or self.compression.lower() == "none"
            or self.compression.lower() == "uncompressed"
        ):
            self.compression = None

        # munge gz to gzip
        if self.compression is not None and self.compression.lower().startswith("gz"):
            self.compression = "gzip"

        # Parse back to a pathlib, because we write paths out to JSON as strings
        if type(self.cache_location) is str:
            self.cache_location = pathlib.Path(self.cache_location)

    def __eq__(self, other):
        if not isinstance(other, Dataset):
            return NotImplemented
        matching_fields = ["name", "format", "compression", "scale_factor", "delim"]

        self_dict = asdict(self)
        self_dict = {k: self_dict[k] for k in self_dict.keys() if k in matching_fields}
        other_dict = asdict(other)
        other_dict = {
            k: other_dict[k] for k in other_dict.keys() if k in matching_fields
        }

        # Munge compression
        self_dict["compression"] = (
            "uncompressed"
            if self_dict["compression"] is None
            or self_dict["compression"].lower() == "none"
            else self_dict["compression"]
        )
        other_dict["compression"] = (
            "uncompressed"
            if other_dict["compression"] is None
            or other_dict["compression"].lower() == "none"
            else other_dict["compression"]
        )

        return self_dict == other_dict

    @classmethod
    def from_json(cls, metadata):
        if isinstance(metadata, str) or isinstance(metadata, pathlib.Path):
            with open(metadata) as f:
                json_dump = json.load(f, object_pairs_hook=OrderedDict)

                # Add the metadata file itself too.
                json_dump["metadata_file"] = pathlib.Path(metadata)

                metadata = json_dump

        # Construct the tables, adding them back in
        # TODO: handle the case where there is a single file and no table attribute?
        tables = metadata.pop("tables", None)
        if tables is not None:
            tables = [Table(**table) for table in tables]
            metadata["tables"] = tables

        return cls(**metadata)

    # For all datasets with this name in the cache, return a list of them
    def list_variants(self):
        # TODO: factor this out into a find helper? Then we can surface that | use that to find all variants extant?
        local_cache_location = config.get_cache_location()

        log.debug(f"Checking local cache at {local_cache_location}")

        metadata_files = local_cache_location.glob(
            f"**/{self.name}/**/{config.metadata_filename}"
        )

        return [Dataset.from_json(ds) for ds in metadata_files]

    def ensure_dataset_loc(self, new_hash="raw"):
        # If this is set, return
        if self.cache_location is not None:
            return self.cache_location

        # otherwise, look for a metadata_file and if that's not there, create a new one
        if self.metadata_file is not None:
            # TODO: fill in the hash too? Is that even needed?
            self.cache_location = self.metadata_file.parent
        else:
            self.hash = new_hash

            self.cache_location = pathlib.Path(
                config.get_cache_location(), self.name, self.hash
            )

        if not self.cache_location.exists():
            self.cache_location.mkdir(parents=True, exist_ok=True)

        return self.cache_location

    def get_extension(self, table):
        ext = os.extsep + self.format
        if self.format == "csv" and self.compression == "gzip":
            ext = ext + os.extsep + "gz"
        return ext

    def ensure_table_loc(self, table=None):
        """This function will get the location of a table to be used + passed to
        a pyarrow dataset. It will ensure that all the directories leading up to
        the files that contain the data all exist (but will not create the data
        files themselves, directly). This function should be used to get the location
        of a table rather than constructing it oneself"""
        dataset_path = self.ensure_dataset_loc()
        # Defaults to the 0th table, which for single-table datasets is exactly what we want
        table = self.get_one_table(table)

        if len(table.files) > 1 or table.multi_file:
            table_path = dataset_path / table.table
            table_path.mkdir(exist_ok=True)
        elif self.name not in tpc_info.tpc_datasets and table.files:
            # If there is a files entry, use it
            table_path = dataset_path / table.files[0]["file_path"]
        else:
            table_path = dataset_path / (table.table + self.get_extension(table))
        return table_path

    def get_one_table(self, table=None):
        if isinstance(table, Table):
            return table

        # get the dataset's tables if there isn't one given
        all_tables = self.tables

        # default to the first table
        index = 0

        if isinstance(table, int):
            index = table
        elif isinstance(table, str):
            index = [x.table for x in all_tables].index(table)
        elif table is None and len(all_tables) > 1:
            warnings.warn(
                "This dataset has more than one table, but a table was not specified only returning the first"
            )
        return all_tables[index]

    # TODO: should these not return the specs, but the dataset itself?
    def get_csv_dataset_spec(self, table):
        # defaults
        po = csv.ParseOptions()
        co = csv.ConvertOptions()
        schema = None
        if self.delim:
            po = csv.ParseOptions(delimiter=self.delim)

        # add extra nulls
        if self.extra_nulls:
            co.null_values = co.null_values + self.extra_nulls

        column_names = None
        if table.schema:
            schema = util.get_arrow_schema(table.schema)
            column_names = list(table.schema.keys())

        ro = csv.ReadOptions(
            column_names=column_names,
            # if column_names are provided, we cannot autogenerate, after the defer to header_line
            autogenerate_column_names=column_names is None and not table.header_line,
        )

        dataset_read_format = pads.CsvFileFormat(
            read_options=ro, parse_options=po, convert_options=co
        )

        return dataset_read_format, schema

    def get_raw_tpc_dataset_spec(self, table):
        column_types = tpc_info.col_dicts[self.name][table.table]

        # dbgen's .tbl output has a trailing delimiter
        column_types_trailed = column_types.copy()
        column_types_trailed["trailing_columns"] = pa.string()

        ro = csv.ReadOptions(
            column_names=column_types_trailed.keys(),
            encoding="iso8859" if self.name == "tpc-ds" else "utf8",
        )

        po = csv.ParseOptions(delimiter="|")

        co = csv.ConvertOptions(
            column_types=column_types_trailed,
            # We should be able to use include_columns here, but I can't
            # seem to get it to work without duplicating all of the columns all over
            # include_columns = list(column_types.keys()),
        )

        dataset_read_format = pads.CsvFileFormat(
            read_options=ro, parse_options=po, convert_options=co
        )

        # return the dataset and then also the schema (though the schema critically
        # does not have the extra column at the end here)
        return dataset_read_format, pa.schema(column_types.copy())

    def get_table_dataset(self, table=None):
        # Defaults to the 0th table, which for single-table datasets is exactly what we want
        table = self.get_one_table(table)

        # Defaults
        schema = table.schema
        if self.format == "parquet":
            dataset_read_format = pads.ParquetFileFormat()
            schema = None
        if self.format == "csv":
            dataset_read_format, schema = self.get_csv_dataset_spec(table)
        if self.format == "tpc-raw":
            dataset_read_format, schema = self.get_raw_tpc_dataset_spec(table)

        return pads.dataset(
            self.ensure_table_loc(table), schema=schema, format=dataset_read_format
        )

    def get_write_format(self, table):
        write_options = None  # Default
        if self.format == "parquet":
            dataset_write_format = pads.ParquetFileFormat()
            write_options = dataset_write_format.make_write_options(
                compression=self.compression,
                # We might want to percolate these up?
                use_deprecated_int96_timestamps=False,
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
            )

        if self.format == "csv":
            dataset_write_format = pads.CsvFileFormat()
            # IFF header_line is False, then add that to the write options
            write_options = dataset_write_format.make_write_options(
                include_header=False if table.header_line is False else True,
                delimiter=self.delim if self.delim else ",",
            )
        return dataset_write_format, write_options

    def download(self):
        log.info("Downloading to cache...")
        down_start = time.perf_counter()

        # Ensure the dataset path is available
        # we can't hash yet, so let's call this "raw"
        cached_dataset_path = self.ensure_dataset_loc(new_hash="raw")

        # For now, we always download all tables. So we need to loop through each table

        for table in self.tables:
            # create table dir
            self.ensure_table_loc(table)

            for file in table.files:
                # the path it will be stored at
                filename = file.get("file_path")
                # we want to use the table_name incase the file stored has a different name than the tablename
                if len(table.files) == 1:
                    dataset_file_path = cached_dataset_path / self.get_table_filename(
                        table
                    )
                else:
                    # TODO: this isn't quite right, but _should_ work
                    dataset_file_path = cached_dataset_path / filename

                # craft the URL (need to be careful since sometimes it will contain the name of the dataset)
                full_path = self.url

                # if the filename is not at the end of full_path, join
                if not full_path.endswith(filename):
                    full_path = full_path + filename

                # TODO: validate checksum, something like:
                # https://github.com/conbench/datalogistik/blob/027169a4194ba2eb27ff37889ad7e541bb4b4036/datalogistik/util.py#L913-L919

                util.download_file(full_path, output_path=dataset_file_path)
                util.set_readonly(dataset_file_path)

        down_time = time.perf_counter() - down_start
        log.debug(f"download took {down_time:0.2f} s")
        log.info("Finished downloading.")

    def fill_metadata_from_files(self):
        # TODO: Should we attempt to find format? That should never mismatch...

        # Find files for tables
        for table in self.tables:
            table_loc = self.ensure_table_loc(table)
            if table_loc.is_file():
                probe_file = table_loc  # for probing parquet compression later
                # one file table
                table.files = [
                    {
                        "file_path": table_loc.relative_to(self.ensure_dataset_loc()),
                        "file_size": table_loc.lstat().st_size,
                        "md5": util.calculate_checksum(table_loc),
                    }
                ]
            elif table_loc.is_dir():
                probe_file = next(table_loc.iterdir())
                # multi file table
                table.files = [
                    {
                        "file_path": subfile.relative_to(self.ensure_dataset_loc()),
                        "file_size": subfile.lstat().st_size,
                        "md5": util.calculate_checksum(subfile),
                    }
                    for subfile in table_loc.iterdir()
                    if subfile.is_file()
                ]
        # Find parquet compression
        if self.format == "parquet":
            file_metadata = pq.ParquetFile(probe_file).metadata
            self.compression = file_metadata.row_group(0).column(0).compression.lower()

        # TODO: add auto-detected csv schemas? I'm not actually sure this is a good idea, but this is how we did it:
        # https://github.com/conbench/datalogistik/blob/027169a4194ba2eb27ff37889ad7e541bb4b4036/datalogistik/util.py#L895-L911

    def write_metadata(self):
        # clean up | ensure fields are populated
        self.fill_metadata_from_files()

        # JSONify
        json_string = self.to_json()

        # establish the metadata_file path
        metadata_file_path = self.cache_location / config.metadata_filename
        self.metadata_file = metadata_file_path

        # write
        if metadata_file_path.exists():
            util.set_readwrite(metadata_file_path)
        with open(metadata_file_path, "w") as metadata_file:
            # TODO: how could we use json.dump(self, metadata_file) while still getting the custom serializer to_json() below?
            metadata_file.write(json_string)

        util.set_readonly(metadata_file_path)
        pass

    def to_json(self):
        self.local_creation_date = (
            datetime.datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")
        )

        dict_repr = asdict(self, dict_factory=util.NoNoneDict)

        return json.dumps(dict_repr, default=str)

    def convert(self, new_dataset):
        log.info(
            f"Converting and caching dataset from {self.format}, "
            f"compression {self.compression} to {new_dataset.format}, "
            f"compression {new_dataset.compression}..."
        )
        conv_start = time.perf_counter()

        try:
            # ensure that we have a new dataset location
            new_dataset_path = new_dataset.ensure_dataset_loc(
                new_hash=util.short_hash()
            )

            # convert each table
            for old_table in self.tables:

                # TODO: possible schema changes here at the table level
                table_pads = self.get_table_dataset(old_table)

                # Make a copy of the original table object. we should overwrite any
                # properties changed by the conversion
                new_table = dataclasses.replace(old_table)
                # TODO: the partitioning properties should be set from what was specified on the cmdline
                new_table.partitioning = None
                new_table.multi_file = None
                new_table.files = []  # will be re-populated after conversion
                # Intuitively, you'd like to remove the schema from the metadata here
                # when converting to parquet (because parquet already stores the schema internally).
                # However, we don't have code to convert a pyarrow schema into JSON yet,
                # so we should keep the schema in the metadata here,
                # in case this dataset will be converted to csv later (otherwise the user-specified
                # JSON schema would be lost)

                if not new_table.dim:
                    # TODO: we should check if these are still valid after conversion
                    nrows = table_pads.count_rows()
                    ncols = len(table_pads.schema.names)
                    new_table.dim = [nrows, ncols]
                else:
                    nrows = new_table.dim[0]

                new_dataset.tables.append(new_table)
                dataset_write_format, write_options = new_dataset.get_write_format(
                    new_table
                )
                output_file = new_dataset.ensure_table_loc(new_table.table)
                if new_dataset.format == "csv" and new_dataset.compression:
                    # Remove compression extension from filename, pads cannot compress on the fly
                    # so we need to compress as an extra step and then we'll add the extension.
                    output_file = output_file.parent / output_file.stem

                # Find a reasonable number to set our rows per row group.
                # and then make sure that max rows per group is less than new_nrows
                # TODO: This should actually be something that takes into account the
                # number of cells by default (rows * cols), and then _also_ be configurable like
                # it is in arrowbench
                # alternatively, when pyarrow exposes size per row group options use that instead.
                if 15625000 <= nrows:
                    maxrpg = 15625000
                else:
                    maxrpg = nrows
                minrpg = maxrpg - 1
                # but if that's 0, set it to None
                if maxrpg == 0:
                    maxrpg = None
                    minrpg = None

                pads.write_dataset(
                    table_pads,
                    output_file,
                    format=dataset_write_format,
                    file_options=write_options,
                    min_rows_per_group=minrpg,
                    max_rows_per_group=maxrpg,
                    file_visitor=util.file_visitor if config.debug else None,
                )

                # TODO: this partitioning flag isn't quite right, we should make a new attribute that encodes whether this is a multi-file table
                if new_table.partitioning is None:
                    # Convert from name.format/part-0.format to simply a file name.format
                    # To stay consistent with downloaded/generated datasets (without partitioning)
                    # TODO: do we want to change this in accordance to tpc-raw?
                    tmp_dir_name = pathlib.Path(f"{output_file}.tmp")
                    os.rename(output_file, tmp_dir_name)
                    os.rename(
                        pathlib.Path(tmp_dir_name, f"part-0.{new_dataset.format}"),
                        output_file,
                    )
                    tmp_dir_name.rmdir()

                # TODO: this probably isn't quite right, we should do something else (use arrow?)
                if new_dataset.format == "csv" and new_dataset.compression:
                    util.compress(
                        output_file, output_file.parent, new_dataset.compression
                    )
                    output_file.unlink()

            # Cleanup, write metadata
            conv_time = time.perf_counter() - conv_start
            log.info("Finished conversion.")
            log.debug(f"conversion took {conv_time:0.2f} s")

            new_dataset.write_metadata()

            util.set_readonly_recurse(output_file)
        except Exception:
            log.error("An error occurred during conversion.")
            util.clean_cache_dir(new_dataset_path)
            raise

        return new_dataset

    def output_result(self):
        output = {"name": self.name, "format": self.format}

        tables = {}
        for table in self.tables:
            tables[table.table] = {
                "path": str(self.ensure_table_loc(table)),
                "dim": table.dim,
            }
        output["tables"] = tables

        return json.dumps(output)

    def fill_in_defaults(self, dataset_for_defaults):
        """overwrites fields that are none with values from the given dataset"""
        for dataset_field in fields(self):
            attr = dataset_field.name
            if (
                getattr(self, attr) is None
                and getattr(dataset_for_defaults, attr) is not None
                and attr != "compression"
            ):
                setattr(self, attr, getattr(dataset_for_defaults, attr))
