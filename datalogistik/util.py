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

import pyarrow as pa
import urllib3
from pyarrow import csv
from pyarrow import dataset as ds

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
def create_cached_dataset_path(name, scale_factor, format, partitioning_nrows):
    local_cache_location = config.get_cache_location()
    scale_factor = f"scalefactor_{scale_factor}" if scale_factor != "" else ""
    partitioning_nrows = f"partitioning_{partitioning_nrows}"
    return pathlib.Path(
        local_cache_location, name, scale_factor, format, partitioning_nrows
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


# Validate that the integrity of the files in the dataset at given path is ok, using the metadata file
def validate(path):
    path = pathlib.Path(path)
    if not path.exists():
        msg = f"Path '{path}' does not exist"
        log.error(msg)
        raise RuntimeError(msg)
    if not path.is_dir():
        msg = f"Path '{path}' is not a directory"
        log.error(msg)
        raise RuntimeError(msg)
    metadata_file = pathlib.Path(path, config.metadata_filename)
    dataset_found = False
    if metadata_file.exists():
        orig_file_listing = json.load(open(metadata_file)).get("files")
        if orig_file_listing:
            dataset_found = True

    if not dataset_found:
        msg = f"No valid dataset was found at '{path}'"
        log.error(msg)
        raise RuntimeError(msg)
    else:
        return validate_files(path, orig_file_listing)


def validate_files(path, file_listing):
    new_file_listing = {}
    add_file_listing(new_file_listing, path)
    new_file_listing = new_file_listing.get("files")
    # we can't perform a simple equality check, because the orig_file_listing does not contain the metadata file
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
            log.error("File integrity compromised: (top:original bottom:new)")
            log.error(orig_file)
            log.error(new_file)
            listings_are_equal = False
    if listings_are_equal:
        log.info("Dataset is valid")
        return True
    else:
        log.error("Dataset is NOT valid!")
        return False


# TODO: consider merging with clean_cache (mostly duplicate code)
def validate_cache():
    cache_root = config.get_cache_location()
    log.info(f"Validating cache at {cache_root}")
    for dirpath, dirnames, filenames in os.walk(cache_root):
        if config.metadata_filename in filenames:
            # Dataset found, validate
            if not validate(dirpath):
                log.info(f"Found invalid cache entry at {dirpath}, pruning...")
                prune_cache_entry(pathlib.Path(dirpath).relative_to(cache_root))


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
    if metadata.get("files"):
        # In this case, the dataset info contained checksums. Check them
        if not validate_files(path, metadata.get("files")):
            clean_cache_dir(path)
            msg = "File integrity check for newly created dataset failed."
            log.error(msg)
            raise RuntimeError(msg)
    else:
        add_file_listing(metadata, path)

    json_string = json.dumps(metadata)
    with open(pathlib.Path(path, config.metadata_filename), "w") as metadata_file:
        metadata_file.write(json_string)


# walk up the directory tree up to the root to find a metadatafile
def valid_metadata_in_parent_dirs(dirpath):
    walking_path = pathlib.Path(dirpath)
    cache_root = config.get_cache_location()
    while walking_path != cache_root:
        metadata_file = pathlib.Path(walking_path, config.metadata_filename)
        if metadata_file.exists():
            with open(metadata_file) as f:
                metadata = json.load(f)
                if metadata.get("files"):
                    return True
        walking_path = walking_path.parent
    return False


# Performs cleanup if something happens while creating an entry in the cache
def clean_cache_dir(path):
    path = pathlib.Path(path)
    log.debug(f"Cleaning incomplete cache entry '{path}'")
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


def clean_cache():
    cache_root = config.get_cache_location()
    log.info(f"Cleaning cache at {cache_root}")
    for dirpath, dirnames, _ in os.walk(cache_root):
        if pathlib.Path(dirpath) == cache_root:
            continue
        if not dirnames:
            if not valid_metadata_in_parent_dirs(dirpath):
                clean_cache_dir(dirpath)


def prune_cache_entry(sub_path):
    log.debug(f"Pruning cache entries below cache subdir '{sub_path}'")
    local_cache_location = config.get_cache_location()
    cache_root = pathlib.Path(local_cache_location)
    path = pathlib.Path(cache_root, sub_path)
    if not path.exists():
        msg = f"Path '{path}' does not exist"
        log.error(msg)
        raise RuntimeError(msg)
    if not path.is_dir():
        msg = f"Path '{path}' is not a directory"
        log.error(msg)
        raise RuntimeError(msg)

    valid_dataset = False
    metadata_file = pathlib.Path(path, config.metadata_filename)
    if metadata_file.exists():
        with open(metadata_file) as f:
            if json.load(f).get("files"):
                valid_dataset = True

    if not valid_dataset:
        # check if this path is a subdir of a valid dataset
        if valid_metadata_in_parent_dirs(path.parent):
            msg = f"Path '{path}' seems to be a subdirectory of a valid dataset, refusing to remove it."
            log.error(msg)
            raise RuntimeError(msg)

    log.info(f"Pruning Directory {path}")

    # Delete the cache entry itself
    shutil.rmtree(path, ignore_errors=True)

    # Use util function to clean up any superfluous directories
    clean_cache_dir(path)


def schema_to_dict(schema):
    field_dict = {}
    for field in schema:
        field_dict[field.name] = str(field.type)
    return field_dict


# Create Arrow Dataset for a given input file
def get_dataset(input_file, dataset_info, table_name=None):
    column_list = None  # Default
    if dataset_info["format"] == "parquet":
        dataset_read_format = ds.ParquetFileFormat()
    if dataset_info["format"] == "csv":
        # defaults
        po = csv.ParseOptions()
        ro = csv.ReadOptions()
        co = csv.ConvertOptions()

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
                column_names=column_types_trailed.keys(), encoding="ISO-8859"
            )
            co = csv.ConvertOptions(column_types=column_types_trailed)

        dataset_read_format = ds.CsvFileFormat(
            read_options=ro, parse_options=po, convert_options=co
        )

    dataset = ds.dataset(input_file, format=dataset_read_format)
    scanner = dataset.scanner(columns=column_list)
    return dataset, scanner


# Convert a cached dataset into another format, return the new directory path
def convert_dataset(
    dataset_info,
    parquet_compression,
    old_format,
    new_format,
    old_nrows,
    new_nrows,
):
    log.info(
        f"Converting and caching dataset from {old_format}, {old_nrows} rows per "
        f"partition to {new_format}, {new_nrows} rows per partition..."
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
        dataset_name, scale_factor, old_format, str(old_nrows)
    )
    cached_dataset_metadata_file = pathlib.Path(
        cached_dataset_path, config.metadata_filename
    )
    if not cached_dataset_metadata_file.exists():
        msg = f"Could not find source dataset at {str(cached_dataset_metadata_file)}"
        log.error(msg)
        raise ValueError(msg)

    with open(cached_dataset_metadata_file) as f:
        dataset_metadata = json.load(f)

    if (dataset_metadata["format"] == new_format) and (old_nrows == new_nrows):
        log.info("Conversion not needed.")
        return cached_dataset_path

    metadata_table_list = []
    try:
        output_dir = create_cached_dataset_path(
            dataset_name, scale_factor, new_format, str(new_nrows)
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        for file_name in file_names:
            input_file = pathlib.Path(cached_dataset_path, f"{file_name}.{old_format}")
            output_file = pathlib.Path(output_dir, f"{file_name}.{new_format}")

            dataset, scanner = get_dataset(input_file, dataset_metadata, file_name)

            write_options = None  # Default
            if new_format == "parquet":
                dataset_write_format = ds.ParquetFileFormat()
                if parquet_compression is None:
                    parquet_compression = "snappy"  # Use snappy by default
                write_options = dataset_write_format.make_write_options(
                    compression=parquet_compression
                )
            if new_format == "csv":
                dataset_write_format = ds.CsvFileFormat()

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
                    "table": f"{file_name}.{new_format}",
                    "schema": schema_to_dict(dataset.schema),
                }
            )

            # TODO: The dataset API does a poor job at detecting the schema.
            # Would be nice to be able to fall back to read/write_csv etc.
            # Another option is to store the schema as metadata in the repo and pass it
            # to dataset

        conv_time = time.perf_counter() - conv_start
        log.info("Finished conversion.")
        log.debug(f"conversion took {conv_time:0.2f} s")
        dataset_info["tables"] = metadata_table_list
        dataset_info["format"] = new_format
        dataset_info["partitioning-nrows"] = new_nrows
        if parquet_compression is not None:
            dataset_info["parquet-compression"] = parquet_compression
        write_metadata(dataset_info, output_dir)

    except Exception:
        log.error("An error occurred during conversion.")
        clean_cache_dir(output_dir)
        raise

    return output_dir


def generate_dataset(dataset_info, argument_info):
    dataset_name = argument_info.dataset
    log.info(f"Generating {dataset_name} data to cache...")
    gen_start = time.perf_counter()
    cached_dataset_path = create_cached_dataset_path(
        dataset_name,
        argument_info.scale_factor,
        dataset_info["format"],
        str(dataset_info["partitioning-nrows"]),
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

        metadata_table_list = []
        for table in tpc_info.tpc_table_names[dataset_name]:
            input_file = pathlib.Path(cached_dataset_path, table + ".csv")
            dataset, scanner = get_dataset(input_file, dataset_info, table)
            metadata_table_list.append(
                {"table": table + ".csv", "schema": schema_to_dict(dataset.schema)}
            )

        gen_time = time.perf_counter() - gen_start
        log.info("Finished generating.")
        log.debug(f"generation took {gen_time:0.2f} s")
        dataset_info["tables"] = metadata_table_list
        write_metadata(dataset_info, cached_dataset_path)

    except Exception:
        log.error("An error occurred during generation.")
        clean_cache_dir(cached_dataset_path)
        raise

    return cached_dataset_path


def decompress(cached_dataset_path, dataset_file_name, compression):
    if compression is None:
        return
    log.info("Decompressing dataset in cache...")
    decomp_start = time.perf_counter()
    if compression == "gz":
        compressed_file_name = dataset_file_name
        compressed_file_path = pathlib.Path(cached_dataset_path, compressed_file_name)
        decompressed_file_path = removesuffix(compressed_file_path, ".gz")
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
        clean_cache_dir(cached_dataset_path)
        raise ValueError(msg)
    decomp_time = time.perf_counter() - decomp_start
    log.info("Finished decompressing.")
    log.debug(f"decompression took {decomp_time:0.2f} s")


def download_dataset(dataset_info, argument_info):
    log.info("Downloading to cache...")
    down_start = time.perf_counter()
    cached_dataset_path = create_cached_dataset_path(
        argument_info.dataset,
        "",  # no scale_factor
        dataset_info["format"],
        str(dataset_info["partitioning-nrows"]),
    )
    cached_dataset_path.mkdir(parents=True, exist_ok=True)

    dataset_file_name = dataset_info["url"].split("/")[-1]
    dataset_file_path = pathlib.Path(cached_dataset_path, dataset_file_name)

    # If the dataset file already exists, remove it.
    # It doesn't have a metadata file (otherwise, the cache would have hit),
    # so something could have gone wrong while downloading/converting previously
    if dataset_file_path.exists():
        log.debug(f"Removing existing file '{dataset_file_path}'")
        dataset_file_path.rmdir()
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

    # Decompress if necessary
    if "file-compression" in dataset_info:
        compression = dataset_info["file-compression"]
        decompress(cached_dataset_path, dataset_file_name, compression)
        dataset_file_name = removesuffix(dataset_file_name, "." + compression)
        dataset_file_path = removesuffix(dataset_file_path, "." + compression)

    try:
        dataset, scanner = get_dataset(dataset_file_path, dataset_info)
        dataset_info["tables"] = [
            {"table": str(dataset_file_name), "schema": schema_to_dict(dataset.schema)}
        ]
        write_metadata(dataset_info, cached_dataset_path)

    except Exception:
        log.error("pyarrow.dataset is unable to read downloaded file")
        clean_cache_dir(cached_dataset_path)
        raise

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
