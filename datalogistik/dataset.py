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

import concurrent
import datetime
import json
import os
import pathlib
import time
import warnings
from collections import OrderedDict
from dataclasses import asdict, dataclass, field, fields, replace
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

    def get_extension(self):
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
        else:
            table_path = dataset_path / (table.table + self.get_extension())
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

    def file_listing_item(self, file_path):
        rel_path = file_path.relative_to(self.ensure_dataset_loc())
        file_size = os.path.getsize(file_path)
        file_md5 = util.calculate_checksum(file_path)
        return {
            "rel_path": rel_path.as_posix(),
            "file_size": file_size,
            "md5": file_md5,
        }

    def create_file_listing(self, table):
        """Create a file listing for the given table with relative paths, file sizes and md5 checksums."""
        path = self.ensure_table_loc(table)
        if path.is_file():
            # Single-file dataset, no parallelism needed
            return [self.file_listing_item(path)]
        with concurrent.futures.ProcessPoolExecutor(config.get_thread_count()) as pool:
            futures = []
            for cur_path, dirs, files in os.walk(path):
                for file_name in files:
                    futures.append(
                        pool.submit(
                            self.file_listing_item, pathlib.Path(cur_path, file_name)
                        )
                    )
            file_list = []
            for f in futures:
                file_list.append(f.result())
        return sorted(file_list, key=lambda item: item["rel_path"])

    def get_file_listing_tuple(self, table):
        """Helper function for parallel creation of listings;
        returns both the file listing and the table it belongs to."""
        return table.table, self.create_file_listing(table)

    def validate_table_files(self, table):
        """Validate the files of the given table in this dataset using the file metadata attached to it.
        Returns true if the files passed the integrity check or if there are no checksums attached.
        If files or checksums are missing in the metadata, they are assumed to be ok."""
        if not table.files:
            log.info(
                f"No metadata found for table {table}, could not perform validation (assuming valid)"
            )
            return True

        new_file_listing = self.create_file_listing(table)
        # we can't perform a simple equality check on the whole listing,
        # because the orig_file_listing does not contain the metadata file.
        # Also, it would be nice to show the user which files failed.
        listings_are_equal = True
        for orig_file in table.files:
            if not orig_file.get("md5"):
                log.info(
                    f"No checksum found for file {orig_file}, could not perform validation (assuming valid)"
                )
                continue
            found = None
            for new_file in new_file_listing:
                if new_file["rel_path"] == orig_file["rel_path"]:
                    found = new_file
                    break

            if found is None:
                orig_file_path = orig_file["rel_path"]
                log.error(f"Missing file: {orig_file_path}")
                listings_are_equal = False
            elif orig_file != new_file:
                log.error(
                    "File integrity compromised: (top:properties in metadata bottom:calculated properties)"
                )
                log.error(orig_file)
                log.error(new_file)
                listings_are_equal = False

        log.debug(
            f"Table {table.table} is{'' if listings_are_equal else ' NOT'} valid!"
        )
        return listings_are_equal

    def validate(self):
        """Validate that the integrity of the files in this dataset is ok, using the metadata.
        Returns true if the dataset passed the integrity check or if there are no checksums attached.
        If tables or checksums of files are missing in the metadata, they are assumed to be ok.
        However, if there is a checksum for a file in the metadata but that file does not exist,
        this function will return False (invalid)"""
        if not self.tables:
            log.info(
                "No tables metadata found, could not perform validation (assuming valid)"
            )
            return True
        if len(self.tables) <= 1:
            dataset_valid = self.validate_table_files(self.tables[0])
        else:
            with concurrent.futures.ProcessPoolExecutor(
                config.get_thread_count()
            ) as pool:
                futures = []
                for table in self.tables:
                    futures.append(pool.submit(self.validate_table_files, table))
                validation_results = []
                for f in futures:
                    validation_results.append(f.result())
            dataset_valid = False not in validation_results
        log.info(f"Dataset is{'' if dataset_valid else ' NOT'} valid")
        return dataset_valid

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
                delimiter=self.delim,
            )
        return dataset_write_format, write_options

    def download(self):
        log.info("Downloading to cache...")
        down_start = time.perf_counter()

        # Ensure the dataset path is available
        # we can't hash yet, so let's call this "raw"
        self.ensure_dataset_loc(new_hash="raw")

        # For now, we always download all tables. So we need to loop through each table
        for table in self.tables:
            # create table dir
            table_path = self.ensure_table_loc(table)

            for file in table.files:
                download_path = table_path
                if len(table.files) == 1:
                    url = self.url
                else:
                    # contains the suffix for the download url
                    file_name = file.get("rel_path")
                    if file_name:
                        # All files constituting a table must be in a dir with name table.name (created by ensure_table_loc)
                        download_path = table_path / pathlib.Path(file_name).name
                        url = self.url + "/" + pathlib.Path(file_name).name

                util.download_file(url, output_path=download_path)
                util.set_readonly(download_path)

            # Try validation in case the dataset info contained checksums
            if not self.validate_table_files(table):
                util.clean_cache_dir(self.cache_location)
                msg = "File integrity check for newly downloaded dataset failed."
                log.error(msg)
                raise RuntimeError(msg)

        down_time = time.perf_counter() - down_start
        log.debug(f"download took {down_time:0.2f} s")
        log.info("Finished downloading.")

    def fill_metadata_from_files(self):
        # TODO: Should we attempt to find format? That should never mismatch...

        if len(self.tables) == 1:
            self.tables[0].files = self.create_file_listing(self.tables[0])
        else:
            with concurrent.futures.ProcessPoolExecutor(
                config.get_thread_count()
            ) as pool:
                futures = []
                for table in self.tables:
                    futures.append(pool.submit(self.get_file_listing_tuple, table))
                for f in futures:
                    table, listing = f.result()
                    self.get_one_table(table).files = listing

        # Find parquet compression
        if self.format == "parquet":
            first_file = self.cache_location / self.tables[0].files[0]["rel_path"]
            file_metadata = pq.ParquetFile(first_file).metadata
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
                new_table = replace(old_table)
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
