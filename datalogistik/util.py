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
import gzip
import hashlib
import json
import os
import pathlib
import shutil
import time
from collections import OrderedDict
from collections.abc import Mapping

import pyarrow as pa
import urllib3
from pyarrow import csv
from pyarrow import dataset as ds
from pyarrow import parquet as pq

from . import config, tpc_info
from .log import log
from .tpc_builders import DBGen, DSDGen


def removesuffix(orig_path, suffix):
    path = pathlib.Path(orig_path)
    if path.suffix == suffix:
        return path.parent / path.stem
    else:
        return orig_path


def peek_line(fd):
    pos = fd.tell()
    line = fd.readline()
    fd.seek(pos)
    return line


def file_visitor(written_file):
    log.debug(f"path={written_file.path}")
    log.debug(f"metadata={written_file.metadata}")


# Construct a path to a dataset entry in the cache (possibly not existing yet)
def create_cached_dataset_path(
    name, scale_factor, format, partitioning_nrows, compression
):
    local_cache_location = config.get_cache_location()
    scale_factor = f"scalefactor_{scale_factor}" if scale_factor else ""
    partitioning_nrows = f"partitioning_{partitioning_nrows}"
    parquet_compression_str = f"compression_{compression}"
    return pathlib.Path(
        local_cache_location,
        name,
        scale_factor,
        format,
        partitioning_nrows,
        parquet_compression_str,
    )


# For each item in the itemlist, add it to metadata if it exists in dataset_info
def add_if_present(itemlist, dataset_info, metadata):
    for item in itemlist:
        if item in dataset_info:
            metadata[item] = dataset_info[item]


def calculate_checksum(file_path):
    with open(file_path, "rb") as f:
        file_hash = hashlib.md5()
        chunk = f.read(config.hashing_chunk_size)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(config.hashing_chunk_size)

    return file_hash.hexdigest()


def add_file_listing(metadata, path):
    file_list = []
    for cur_path, dirs, files in os.walk(path):
        for file_name in files:
            full_path = os.path.join(cur_path, file_name)
            rel_path = os.path.relpath(full_path, path)
            file_size = os.path.getsize(full_path)
            file_md5 = calculate_checksum(full_path)
            file_list.append(
                {"file_path": rel_path, "file_size": file_size, "md5": file_md5}
            )

    metadata["files"] = file_list


# Returns true if the given path contains a dataset with a metadata file that contains
# a file listing.
def contains_dataset(path):
    if not path.exists():
        msg = f"Path '{path}' does not exist"
        log.error(msg)
        raise RuntimeError(msg)
    if not path.is_dir():
        msg = f"Path '{path}' is not a directory"
        log.error(msg)
        raise RuntimeError(msg)

    metadata_file = pathlib.Path(path, config.metadata_filename)
    if metadata_file.exists():
        with open(metadata_file) as f:
            if json.load(f).get("files"):
                return True
    return False


# Validate that the integrity of the files in the dataset at given path is ok, using the metadata file.
# Return true if the dataset passed the integrity check.
def validate(path):
    path = pathlib.Path(path)
    dataset_found = contains_dataset(path)
    if not dataset_found:
        msg = f"No valid dataset was found at '{path}'"
        log.error(msg)
        raise RuntimeError(msg)
    metadata_file = pathlib.Path(path, config.metadata_filename)
    with open(metadata_file) as f:
        orig_file_listing = json.load(f).get("files")
    dataset_valid = validate_files(path, orig_file_listing)
    log.info(f"Dataset at {path} is{'' if dataset_valid else ' NOT'} valid")
    return dataset_valid


# Validate the files in the given path for integrity using the given file listing.
# Return true if the files passed the integrity check.
def validate_files(path, file_listing):
    new_file_listing = {}
    add_file_listing(new_file_listing, path)
    new_file_listing = new_file_listing.get("files")
    # we can't perform a simple equality check on the whole listing,
    # because the orig_file_listing does not contain the metadata file.
    # Also, it would be nice to show the user which files failed.
    listings_are_equal = True
    for orig_file in file_listing:
        found = None
        for new_file in new_file_listing:
            if new_file["file_path"] == orig_file["file_path"]:
                found = new_file
                break
        if found is None:
            orig_file_path = orig_file["file_path"]
            log.error(f"Missing file: {orig_file_path}")
            listings_are_equal = False
        if orig_file != new_file:
            log.error(
                "File integrity compromised: (top:properties in metadata bottom:calculated properties)"
            )
            log.error(orig_file)
            log.error(new_file)
            listings_are_equal = False

        log.debug(f"Dataset is{'' if listings_are_equal else ' NOT'} valid!")
        return listings_are_equal


# Validate all entries in the cache
def validate_cache(remove_failing):
    cache_root = config.get_cache_location()
    log.info(f"Validating cache at {cache_root}")
    for dirpath, dirnames, filenames in os.walk(cache_root):
        if config.metadata_filename in filenames:
            # Dataset found, validate
            if not validate(dirpath):
                log.info(f"Found invalid cache entry at {dirpath}")
                if remove_failing:
                    log.info("Pruning...")
                    prune_cache_entry(pathlib.Path(dirpath).relative_to(cache_root))


# Write the metadata about a newly created dataset instance at path,
# using the information in dataset_info in addition to some locally generated info.
def write_metadata(dataset_info, path):
    metadata = {
        "local-creation-date": datetime.datetime.now()
        .astimezone()
        .strftime("%Y-%m-%dT%H:%M:%S%z")
    }

    # Propagate metadata from dataset_info
    add_if_present(
        [
            "name",
            "format",
            "header-line",
            "partitioning-nrows",
            "scale-factor",
            "dim",
            "delim",
            "url",
            "homepage",
            "tables",
            "parquet-compression",
            "files",
        ],
        dataset_info,
        metadata,
    )
    add_file_listing(metadata, path)

    json_string = json.dumps(metadata)
    with open(pathlib.Path(path, config.metadata_filename), "w") as metadata_file:
        metadata_file.write(json_string)


# Walk up the directory tree up to the root of the cache to find a metadata file.
# Return true if a metadata file is found
def valid_metadata_in_parent_dirs(dirpath):
    walking_path = pathlib.Path(dirpath)
    cache_root = config.get_cache_location()
    while walking_path != cache_root:
        if contains_dataset(walking_path):
            return True
        walking_path = walking_path.parent
    return False


# Performs cleanup if something happens while creating an entry in the cache
def clean_cache_dir(path):
    path = pathlib.Path(path)
    log.debug(f"Cleaning cache directory '{path}'")
    cache_root = config.get_cache_location()
    if cache_root not in path.parents:
        msg = "Refusing to clean a directory outside of the local cache"
        log.error(msg)
        raise RuntimeError(msg)

    # Delete the cache entry itself
    shutil.rmtree(path, ignore_errors=True)

    # Search for parent directories that are empty and should thus be deleted
    while path.parent != cache_root:
        path = path.parent
        try:
            next(path.iterdir())
        except StopIteration:
            log.debug(f"Removing empty parent path '{path}'")
            os.rmdir(path)


# Search the cache for directories that do not have a metadata file
# and are not part of a dataset), and remove them.
def clean_cache():
    cache_root = config.get_cache_location()
    log.info(f"Cleaning cache at {cache_root}")
    for dirpath, dirnames, _ in os.walk(cache_root):
        if pathlib.Path(dirpath) == cache_root:
            continue
        if not dirnames:
            if not valid_metadata_in_parent_dirs(dirpath):
                clean_cache_dir(dirpath)


# Remove an entry from the cache by the given subdir.
def prune_cache_entry(sub_path):
    log.debug(f"Pruning cache entries below cache subdir '{sub_path}'")
    local_cache_location = config.get_cache_location()
    cache_root = pathlib.Path(local_cache_location)
    path = pathlib.Path(cache_root, sub_path)
    if not contains_dataset(path):
        # check if this path is a subdir of a valid dataset
        if valid_metadata_in_parent_dirs(path.parent):
            msg = f"Path '{path}' seems to be a subdirectory of a valid dataset, refusing to remove it."
            log.error(msg)
            raise RuntimeError(msg)

    log.info(f"Pruning Directory {path}")
    shutil.rmtree(path, ignore_errors=True)
    clean_cache_dir(path)


# Convert a pyarrow.schema to a dict that can be serialized to JSON
def schema_to_dict(schema):
    field_dict = {}
    for field in schema:
        field_dict[field.name] = str(field.type)
    return field_dict


def convert_arrow_alias(type_name):
    aliases = {
        "bool": "bool_",
        "halffloat": "float16",
        "float": "float32",
        "double": "float64",
        "decimal": "decimal128",
    }
    return aliases.get(type_name, type_name)


# Create an instance of the pyarrow datatype with the given name
def arrow_type_function_lookup(function_name):
    if isinstance(function_name, str):
        function_name = convert_arrow_alias(function_name)
        pa_type_func = getattr(pa, function_name)
        return pa_type_func

    # The argument was not a pyarrow type (maybe a nested structure?)
    return None


# Convert a given item (string or dict) to the corresponding Arrow datatype
def arrow_type_from_json(input_type):
    arrow_nested_types = {
        "list_",
        "large_list",
        "map_",
        "struct",
        "dictionary",
        # Could be useful for the user to have control over nullability
        "field",
    }

    # In case the type is a simple string
    if isinstance(input_type, str):
        if input_type in arrow_nested_types:
            msg = "Nested types in schema not supported yet"
            log.error(msg)
            raise ValueError(msg)
        return arrow_type_function_lookup(input_type)()

    # Alternatively, a type can be encoded as a name:value pair
    if not input_type.get("type_name"):
        msg = "Schema field type 'type_name' missing"
        log.error(msg)
        raise ValueError(msg)

    type_name = input_type.get("type_name")
    args = input_type.get("arguments")
    if type_name in arrow_nested_types:
        msg = "Nested types in schema not supported yet"
        log.error(msg)
        raise ValueError(msg)

    if args is None:
        return arrow_type_function_lookup(type_name)()
    if isinstance(args, Mapping):
        return arrow_type_function_lookup(type_name)(**args)
    elif isinstance(args, list):
        return arrow_type_function_lookup(type_name)(*args)
    else:  # args is probably a single value
        return arrow_type_function_lookup(type_name)(args)


# Convert the given dict to a pyarrow.schema
def get_arrow_schema(input_schema):
    if input_schema is None:
        return None
    log.debug("Converting schema to pyarrow.schema...")
    field_list = []
    # TODO: a `field()` entry is not a (name, type) tuple
    for (field_name, type) in input_schema.items():
        log.debug(f"Schema: adding field {field_name}")
        arrow_type = arrow_type_from_json(type)
        field_list.append(pa.field(field_name, arrow_type))

    output_schema = pa.schema(field_list)
    return output_schema


# Lookup the user-specified schema or return None is none was found
def get_table_schema_from_metadata(dataset_info, table_name):
    if dataset_info.get("tables"):
        for table_entry in dataset_info.get("tables"):
            if table_name is None or table_entry["table"] == table_name:
                if table_entry.get("schema"):
                    return table_entry["schema"]
                break  # there should be only 1
    return None


# Create Arrow Dataset for a given input file
def get_dataset(input_file, dataset_info, table_name=None):
    # Defaults
    column_list = None
    schema = None
    format = dataset_info["format"]
    if format == "parquet":
        dataset_read_format = ds.ParquetFileFormat()
    if format == "csv":
        # defaults
        po = csv.ParseOptions()
        ro = csv.ReadOptions()
        co = csv.ConvertOptions()
        # TODO: Should we fall-back to read_csv in case schema detection fails?

        if "delim" in dataset_info:
            po = csv.ParseOptions(delimiter=dataset_info["delim"])
        if dataset_info["name"] in tpc_info.tpc_datasets:
            if table_name is None:
                msg = (
                    "dataset is in tpc_datasets but table_name (needed to look up the "
                    "schema) is 'None'"
                )
                log.error(msg)
                raise ValueError(msg)
            column_types = tpc_info.col_dicts[dataset_info["name"]][table_name]
            column_list = list(column_types.keys())

            # dbgen's .tbl output has a trailing delimiter
            column_types_trailed = column_types.copy()
            column_types_trailed["trailing_columns"] = pa.string()
            ro = csv.ReadOptions(
                column_names=column_types_trailed.keys(),
                encoding="iso8859" if dataset_info["name"] == "tpc-ds" else "utf8",
            )
            co = csv.ConvertOptions(column_types=column_types_trailed)
        else:  # not a TPC dataset
            column_names = None
            autogen_column_names = False
            table_schema = get_table_schema_from_metadata(dataset_info, table_name)
            if table_schema:
                log.debug("Found user-specified schema in metadata")
                schema = get_arrow_schema(table_schema)
                column_names = list(table_schema.keys())
            else:
                has_header_line = dataset_info.get("header-line", False)
                autogen_column_names = not has_header_line

            ro = csv.ReadOptions(
                column_names=column_names,
                autogenerate_column_names=autogen_column_names,
            )

        dataset_read_format = ds.CsvFileFormat(
            read_options=ro, parse_options=po, convert_options=co
        )

    dataset = ds.dataset(input_file, schema=schema, format=dataset_read_format)
    scanner = dataset.scanner(columns=column_list)
    return dataset, scanner


# Convert a cached dataset into another format, return the new directory path
def convert_dataset(
    dataset_info,
    old_compression,
    new_compression,
    old_format,
    new_format,
    old_nrows,
    new_nrows,
):
    log.info(
        f"Converting and caching dataset from {old_format}, {old_nrows} rows per "
        f"partition, compression {old_compression} to {new_format}, {new_nrows} rows "
        f"per partition, compression {new_compression}..."
    )
    conv_start = time.perf_counter()
    dataset_name = dataset_info["name"]
    scale_factor = dataset_info.get("scale-factor", "")
    if dataset_name in tpc_info.tpc_datasets:
        file_names = tpc_info.tpc_table_names[dataset_name]
    else:
        dataset_file_name = dataset_info["url"].split("/")[-1]
        file_names = [dataset_file_name.split(".")[0]]
    cached_dataset_path = create_cached_dataset_path(
        dataset_name, scale_factor, old_format, str(old_nrows), old_compression
    )
    cached_dataset_metadata_file = pathlib.Path(
        cached_dataset_path, config.metadata_filename
    )
    if not cached_dataset_metadata_file.exists():
        msg = f"Could not find source dataset at {str(cached_dataset_metadata_file)}"
        log.error(msg)
        raise ValueError(msg)

    with open(cached_dataset_metadata_file) as f:
        dataset_metadata = json.load(f, object_pairs_hook=OrderedDict)

    if (
        (old_format == new_format)
        and (old_nrows == new_nrows)
        and (old_compression == new_compression)
    ):
        log.info("Conversion not needed.")
        return cached_dataset_path

    if (
        (dataset_info["format"] == new_format)
        and (dataset_info["partitioning-nrows"] == new_nrows)
        and (dataset_info.get("file-compression") == new_compression)  # rules out tpc
    ):
        log.info("Re-downloading instead of converting.")
        return download_dataset(dataset_info)

    metadata_table_list = []
    try:
        output_dir = create_cached_dataset_path(
            dataset_name,
            scale_factor,
            new_format,
            str(new_nrows),
            new_compression,
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        for file_name in file_names:
            input_file = pathlib.Path(cached_dataset_path, f"{file_name}.{old_format}")
            if old_format != "parquet" and old_compression:
                input_file = input_file.parent / f"{input_file.name}.{old_compression}"
            output_file = pathlib.Path(output_dir, f"{file_name}.{new_format}")
            if (
                old_format == "csv"
                and new_format == "csv"
                and old_nrows == new_nrows
                and new_compression is None
            ):
                log.info("decompressing without conversion...")
                decompress(input_file, output_file, old_compression)
                continue

            dataset, scanner = get_dataset(input_file, dataset_metadata, file_name)

            write_options = None  # Default
            if new_format == "parquet":
                dataset_write_format = ds.ParquetFileFormat()
                write_options = dataset_write_format.make_write_options(
                    compression=new_compression,
                    use_deprecated_int96_timestamps=False,
                    coerce_timestamps="us",
                    allow_truncated_timestamps=True,
                )
            if new_format == "csv":
                dataset_write_format = ds.CsvFileFormat()
                # Don't include header if there's a known schema
                if old_format == "csv" and (
                    get_table_schema_from_metadata(dataset_info, file_name)
                    or not dataset_info.get("header-line")
                ):
                    write_options = dataset_write_format.make_write_options(
                        include_header=False
                    )
                else:
                    dataset_info["header-line"] = True

            ds.write_dataset(
                scanner,
                output_file,
                format=dataset_write_format,
                file_options=write_options,
                max_rows_per_file=new_nrows,
                max_rows_per_group=new_nrows if new_nrows != 0 else None,
                file_visitor=file_visitor if config.debug else None,
            )
            if new_nrows == 0:
                # Convert from name.format/part-0.format to simply a file name.format
                # To stay consistent with downloaded/generated datasets (without partitioning)
                tmp_dir_name = pathlib.Path(
                    output_file.parent, f"{file_name}.{new_format}.tmp"
                )
                os.rename(output_file, tmp_dir_name)
                os.rename(
                    pathlib.Path(tmp_dir_name, f"part-0.{new_format}"), output_file
                )
                tmp_dir_name.rmdir()

            metadata_table_list.append(
                {
                    "table": file_name,
                    # This inferred schema is different from a user-specified schema
                    "inferred-schema": schema_to_dict(dataset.schema),
                }
            )
            if new_format == "csv" and new_compression:
                compressed_output_file = (
                    output_file.parent / f"{output_file.name}.{new_compression}"
                )
                compress(output_file, compressed_output_file, new_compression)
                output_file.unlink()

        conv_time = time.perf_counter() - conv_start
        log.info("Finished conversion.")
        log.debug(f"conversion took {conv_time:0.2f} s")
        # Parquet already stores the schema internally
        if new_format == "csv":
            # Don't insert inferred-schema if a known schema is available already
            if dataset_info.get("tables") is None:
                dataset_info["tables"] = metadata_table_list
        dataset_info["format"] = new_format
        dataset_info["partitioning-nrows"] = new_nrows
        dataset_info["parquet-compression"] = new_compression
        if dataset_info.get("files"):
            # Remove the old file listing, because it is not valid for the new dataset
            # (write_metadata will generate and add a new file listing with checksums)
            del dataset_info["files"]
        write_metadata(dataset_info, output_dir)

    except Exception:
        log.error("An error occurred during conversion.")
        clean_cache_dir(output_dir)
        raise

    return output_dir


# Generate a dataset by calling one of the supported external generators
def generate_dataset(dataset_info, argument_info):
    dataset_name = argument_info.dataset
    log.info(f"Generating {dataset_name} data to cache...")
    gen_start = time.perf_counter()
    cached_dataset_path = create_cached_dataset_path(
        dataset_name,
        argument_info.scale_factor,
        dataset_info["format"],
        str(dataset_info["partitioning-nrows"]),
        None,
    )
    cached_dataset_path.mkdir(parents=True, exist_ok=True)

    # Call generator
    generators = {"tpc-h": DBGen, "tpc-ds": DSDGen}
    try:
        generator_class = generators[dataset_name]
        generator = generator_class(executable_path=argument_info.generator_path)
        generator.create_dataset(
            out_dir=cached_dataset_path, scale_factor=argument_info.scale_factor
        )

        # If the entry in the repo file does not specify the schema, try to detect it
        if not dataset_info.get("tables"):
            metadata_table_list = []
            for table in tpc_info.tpc_table_names[dataset_name]:
                input_file = pathlib.Path(cached_dataset_path, table + ".csv")
                try:
                    dataset, scanner = get_dataset(input_file, dataset_info, table)
                    metadata_table_list.append(
                        {
                            "table": table,
                            # TODO: this schema is not inferred, but it does not have
                            # the same structure of a user-specified schema either
                            "schema": schema_to_dict(dataset.schema),
                        }
                    )
                except Exception:
                    log.error(
                        f"pyarrow.dataset is unable to read schema from generated file {input_file}"
                    )
                    clean_cache_dir(cached_dataset_path)
                    raise

        dataset_info["tables"] = metadata_table_list

        gen_time = time.perf_counter() - gen_start
        log.info("Finished generating.")
        log.debug(f"generation took {gen_time:0.2f} s")
        write_metadata(dataset_info, cached_dataset_path)

    except Exception:
        log.error("An error occurred during generation.")
        clean_cache_dir(cached_dataset_path)
        raise

    return cached_dataset_path


def compress(uncompressed_file_path, compressed_file_path, compression):
    if compression is None:
        return
    if compression == "gz":
        log.debug(
            f"Compressing GZip file {uncompressed_file_path} into "
            f"{compressed_file_path}"
        )
        with open(uncompressed_file_path, "rb") as input_file:
            with gzip.open(compressed_file_path, "wb") as output_file:
                shutil.copyfileobj(input_file, output_file)
    else:
        msg = f"Unsupported compression type ({compression})."
        log.error(msg)
        clean_cache_dir(compressed_file_path.parent)
        raise ValueError(msg)


def decompress(compressed_file_path, decompressed_file_path, compression):
    if compression is None:
        return
    if compression == "gz":
        log.debug(
            f"Decompressing GZip file {compressed_file_path} into "
            f"{decompressed_file_path}"
        )
        with gzip.open(compressed_file_path, "rb") as input_file:
            with open(decompressed_file_path, "wb") as output_file:
                shutil.copyfileobj(input_file, output_file)
    else:
        msg = f"Unsupported compression type ({compression})."
        log.error(msg)
        clean_cache_dir(decompressed_file_path.parent)
        raise ValueError(msg)


def download_dataset(dataset_info):
    log.info("Downloading to cache...")
    down_start = time.perf_counter()
    if dataset_info["format"] == "parquet":
        compression = (
            "unknown"  # Temporary location until we detect the actual compression
        )
    else:
        compression = dataset_info["file-compression"]
    cached_dataset_path = create_cached_dataset_path(
        dataset_info["name"],
        "",  # no scale_factor
        dataset_info["format"],
        str(dataset_info["partitioning-nrows"]),
        compression,
    )
    cached_dataset_path.mkdir(parents=True, exist_ok=True)

    dataset_file_name = dataset_info["url"].split("/")[-1]
    dataset_file_path = pathlib.Path(cached_dataset_path, dataset_file_name)

    # If the dataset file already exists, remove it.
    # It doesn't have a metadata file (otherwise, the cache would have hit),
    # so something could have gone wrong while downloading/converting previously
    if dataset_file_path.exists():
        log.debug(f"Removing existing file '{dataset_file_path}'")
        dataset_file_path.unlink()
    url = dataset_info["url"]
    try:
        http = urllib3.PoolManager()
        with http.request("GET", url, preload_content=False) as r, open(
            dataset_file_path, "wb"
        ) as out_file:
            shutil.copyfileobj(r, out_file)  # Performs a chunked copy
    except Exception:
        log.error(f"Unable to download from '{url}'")
        clean_cache_dir(cached_dataset_path)
        raise
    down_time = time.perf_counter() - down_start
    log.debug(f"download took {down_time:0.2f} s")
    log.info("Finished downloading.")

    # Find parquet compression, move to the proper subdir
    if dataset_info["format"] == "parquet":
        md = pq.ParquetFile(dataset_file_path).metadata
        pq_compression = md.row_group(0).column(0).compression.lower()
        pq_compression = f"compression_{pq_compression}"
        cached_dataset_path.replace(cached_dataset_path.parent / pq_compression)
        cached_dataset_path = cached_dataset_path.parent / pq_compression
        dataset_file_path = pathlib.Path(cached_dataset_path, dataset_file_name)

    # Parquet already stores the schema internally
    if dataset_info["format"] == "csv":
        # If the entry in the repo file does not specify the schema, try to detect it
        if not dataset_info.get("tables"):
            try:
                dataset, scanner = get_dataset(dataset_file_path, dataset_info)
                dataset_info["tables"] = [
                    {
                        "table": str(pathlib.Path(dataset_file_name).stem),
                        "inferred-schema": schema_to_dict(dataset.schema),
                    }
                ]
            except Exception:
                log.error(
                    "pyarrow.dataset is unable to read schema from downloaded file"
                )
                clean_cache_dir(cached_dataset_path)
                raise

    if dataset_info.get("files"):
        # In this case, the dataset info contained checksums. Check them
        if not validate_files(cached_dataset_path, dataset_info.get("files")):
            clean_cache_dir(cached_dataset_path)
            msg = "File integrity check for newly created dataset failed."
            log.error(msg)
            raise RuntimeError(msg)

    write_metadata(dataset_info, cached_dataset_path)
    return cached_dataset_path


def copy_from_cache(cached_dataset_path, name):
    log.info("Copying dataset from cache...")
    copy_start = time.perf_counter()
    dest_path = f"./{name}"
    cached_dataset_path.mkdir(parents=True, exist_ok=True)
    # This also copies the metadata file
    shutil.rmtree(dest_path, ignore_errors=True)
    shutil.copytree(cached_dataset_path, dest_path)
    copy_time = time.perf_counter() - copy_start
    log.info("Finished Copying.")
    log.debug(f"copy took {copy_time:0.2f} s")
