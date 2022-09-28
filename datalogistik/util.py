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
import gzip
import hashlib
import json
import os
import pathlib
import shutil
import time
import uuid
from collections.abc import Mapping

import pyarrow as pa
import urllib3

from . import config, tpc_info
from .log import log
from .table import Table
from .tpc_builders import DBGen, DSDGen


# Set file permissions of given path to readonly
def set_readonly(path):
    os.chmod(path, 0o444)


# Set file permissions of given path to readonly
def set_readwrite(path):
    os.chmod(path, 0o666)


# Recursively set file permissions of given path to readonly
def set_readonly_recurse(path):
    for dirpath, dirnames, filenames in os.walk(path):
        os.chmod(dirpath, 0o555)
        for filename in filenames:
            set_readonly(os.path.join(dirpath, filename))


# Recursively set file permissions of given path to readonly
def set_readwrite_recurse(path):
    for dirpath, dirnames, filenames in os.walk(path):
        os.chmod(dirpath, 0o777)
        for filename in filenames:
            set_readwrite(os.path.join(dirpath, filename))


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


def file_listing_item(dataset_path, file_path):
    rel_path = os.path.relpath(file_path, dataset_path)
    file_size = os.path.getsize(file_path)
    file_md5 = calculate_checksum(file_path)
    return {"file_path": rel_path, "file_size": file_size, "md5": file_md5}


def add_file_listing(metadata, path):
    with concurrent.futures.ProcessPoolExecutor(config.get_thread_count()) as pool:
        futures = []
        for cur_path, dirs, files in os.walk(path):
            for file_name in files:
                futures.append(
                    pool.submit(
                        file_listing_item, path, os.path.join(cur_path, file_name)
                    )
                )
        file_list = []
        for f in futures:
            file_list.append(f.result())
    metadata["files"] = sorted(file_list, key=lambda item: item["file_path"])


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
    set_readwrite_recurse(path)
    shutil.rmtree(path, ignore_errors=True)

    # Search for parent directories that are empty and should thus be deleted
    while path.parent != cache_root:
        path = path.parent
        try:
            next(path.iterdir())
        except StopIteration:
            log.debug(f"Removing empty parent path '{path}'")
            os.rmdir(path)


# Search the cache for directories that are not part of a dataset, and remove them.
# Also removes directories that do not have any datasets underneath them.
def clean_cache():
    cache_root = config.get_cache_location()
    log.info(f"Cleaning cache at {cache_root}")
    cleaned_leaf_dir = True
    while cleaned_leaf_dir:
        cleaned_leaf_dir = False
        for dirpath, dirnames, _ in os.walk(cache_root):
            if pathlib.Path(dirpath) == cache_root:
                continue
            if not dirnames:
                if not valid_metadata_in_parent_dirs(dirpath):
                    clean_cache_dir(dirpath)
                    cleaned_leaf_dir = True


# Remove an entry from the cache by the given subdir.
def prune_cache_entry(sub_path):
    log.debug(f"Pruning cache entries below cache subdir '{sub_path}'")
    cache_root = config.get_cache_location()
    path = pathlib.Path(cache_root, sub_path)
    if not path.exists():
        log.info("Could not find entry in cache.")
        return
    if not contains_dataset(path):
        # check if this path is a subdir of a valid dataset
        if valid_metadata_in_parent_dirs(path.parent):
            msg = f"Path '{path}' seems to be a subdirectory of a valid dataset, refusing to remove it."
            log.error(msg)
            raise RuntimeError(msg)

    log.info(f"Pruning Directory {path}")
    set_readwrite_recurse(path)
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


# Convert between max rows per partition and number of partitions
def convert_maxrows_parts(tpc_name, scale_factor, parts_or_rows):
    # nr of rows in the largest table at sf=1
    tpch_rows_per_sf = {"tpc-h": 6000000, "tpc-ds": 1440000}
    if parts_or_rows <= 0:
        return 0
    return int((tpch_rows_per_sf[tpc_name] * float(scale_factor)) / parts_or_rows)


# Generate a dataset by calling one of the supported external generators
# TODO: Generator output cannot be used as dataset output directly, because of the
# trailing columns.
def generate_dataset(dataset):
    log.info(f"Generating {dataset.name} data to cache...")
    gen_start = time.perf_counter()
    # This naming assumes the scale factor is always peresent, which is true for TPC-H but possibly not all generated datasets
    dataset_path = dataset.ensure_dataset_loc(new_hash=f"raw_{dataset.scale_factor}")
    generators = {"tpc-h": DBGen, "tpc-ds": DSDGen}

    # override the format, since we only know how to directly generate tpc-raw format
    dataset.format = "tpc-raw"

    try:
        generator_class = generators[dataset.name]
        # TODO: support executable_path as env var?
        generator = generator_class(executable_path=None)

        dataset_path.mkdir(parents=True, exist_ok=True)
        generator.create_dataset(
            out_dir=dataset_path,
            scale_factor=dataset.scale_factor,
            partitions=config.get_thread_count(),
        )

        metadata_table_list = []
        for table in tpc_info.tpc_table_names[dataset.name]:
            metadata_table_list.append(
                Table(
                    table=table,
                    # These will always be multi_file, so we should code that
                    multi_file=True,
                    # TODO: is this line necessary?
                    # this schema is not inferred, but it does not have
                    # the same structure of a user-specified schema either
                    # "schema": schema_to_dict(dataset.schema),
                )
            )
        dataset.tables = metadata_table_list

        gen_time = time.perf_counter() - gen_start
        log.info("Finished generating.")
        log.debug(f"generation took {gen_time:0.2f} s")
        dataset.fill_metadata_from_files()
        dataset.write_metadata()

    except Exception:
        log.error("An error occurred during generation.")
        clean_cache_dir(dataset_path)
        raise

    return dataset


def compress(uncompressed_file_path, output_dir, compression):
    if (
        compression is None
        or compression.lower() == "none"
        or compression == "uncompressed"
    ):
        return
    if compression == "gzip":
        log.debug(
            f"Compressing GZip dataset {uncompressed_file_path} into " f"{output_dir}"
        )
        if uncompressed_file_path.is_dir():
            file_list = []
            for x in uncompressed_file_path.iterdir():
                if x.is_file():
                    file_list.append(x)
        else:
            file_list = [uncompressed_file_path]
        for uncompressed_file in file_list:
            with open(uncompressed_file, "rb") as input_file:
                with gzip.open(
                    output_dir / (uncompressed_file.name + ".gz"), "wb"
                ) as output_file:
                    shutil.copyfileobj(input_file, output_file)
    else:
        msg = f"Unsupported compression type ({compression})."
        log.error(msg)
        clean_cache_dir(output_dir.parent)
        raise ValueError(msg)


def decompress(compressed_file_path, output_dir, compression):
    if (
        compression is None
        or compression is None
        or compression.lower() == "none"
        or compression == "uncompressed"
    ):
        return
    if compression == "gzip":
        log.debug(
            f"Decompressing GZip dataset {compressed_file_path} into " f"{output_dir}"
        )
        if compressed_file_path.is_dir():
            file_list = []
            for x in compressed_file_path.iterdir():
                if x.is_file():
                    file_list.append(x)
        else:
            file_list = [compressed_file_path]
        for compressed_file in file_list:
            with gzip.open(compressed_file, "rb") as input_file:
                with open(output_dir / compressed_file.stem, "wb") as output_file:
                    shutil.copyfileobj(input_file, output_file)
    else:
        msg = f"Unsupported compression type ({compression})."
        log.error(msg)
        clean_cache_dir(output_dir.parent)
        raise ValueError(msg)


def download_file(url, output_path):
    # If the dataset file already exists, remove it.
    # It doesn't have a metadata file (otherwise, the cache would have hit),
    # so something could have gone wrong while downloading/converting previously
    if output_path.exists():
        log.debug(f"Removing existing path '{output_path}'")
        shutil.rmtree(output_path, ignore_errors=True)

    try:
        http = urllib3.PoolManager()
        with http.request("GET", url, preload_content=False) as r, open(
            output_path, "wb"
        ) as out_file:
            shutil.copyfileobj(r, out_file)  # Performs a chunked copy
    except Exception:
        log.error(f"Unable to download from '{url}'")
        # TODO: cleanup
        raise

    return output_path


def short_hash():
    return uuid.uuid4().hex[1:8]


# ignore None and [] type values
class NoNoneDict(dict):
    def __init__(self, data):
        super().__init__(x for x in data if x[1] is not None and x[1] != [])
