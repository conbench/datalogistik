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
import pyarrow.fs
import urllib3

from . import config, tpc_info
from .log import log
from .table import Table
from .tpc_builders import DBGen, DSDGen


def set_readonly(path):
    """Set file permissions of given path to readonly"""
    os.chmod(path, 0o444)


def set_readwrite(path):
    """Set file permissions of given path to read/write"""
    os.chmod(path, 0o666)


def set_readonly_recurse(path):
    """Recursively set file permissions of given path to readonly"""
    for dirpath, dirnames, filenames in os.walk(path):
        os.chmod(dirpath, 0o555)
        for filename in filenames:
            set_readonly(os.path.join(dirpath, filename))


def set_readwrite_recurse(path):
    """Recursively set file permissions of given path to read/write"""
    if os.path.isfile(path):
        set_readwrite(path)
    for dirpath, dirnames, filenames in os.walk(path):
        os.chmod(dirpath, 0o777)
        for filename in filenames:
            set_readwrite(os.path.join(dirpath, filename))


def file_visitor(written_file):
    log.debug(f"path={written_file.path}")
    log.debug(f"metadata={written_file.metadata}")


def calculate_checksum(file_path):
    """Calculate an md5 checksum of the file at given path"""
    with open(file_path, "rb") as f:
        file_hash = hashlib.md5()
        chunk = f.read(config.hashing_chunk_size)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(config.hashing_chunk_size)

    return file_hash.hexdigest()


def contains_dataset(path) -> bool:
    """Returns true if the given path contains a dataset with a metadata file that
    contains a `table` entry (in which case it is assumed to be defined well enough
    to be used). No file validation is performed."""

    if not path.exists():
        msg = f"Path '{path}' does not exist"
        log.error(msg)
        raise RuntimeError(msg)
    if not path.is_dir():
        msg = f"Path '{path}' is not a directory"
        log.error(msg)
        raise RuntimeError(msg)

    # We can't use Dataset.from_json because it would create a circular dependency
    metadata_file = pathlib.Path(path, config.metadata_filename)
    if metadata_file.exists():
        with open(metadata_file) as f:
            if json.load(f).get("tables"):
                return True
    return False


def valid_metadata_in_parent_dirs(dirpath) -> bool:
    """Walk up the directory tree up to the root of the cache to find a metadata file.
    Return true if a metadata file is found, meaning that the given path is a subdir
    of a dataset."""

    walking_path = pathlib.Path(dirpath)
    cache_root = config.get_cache_location()
    while walking_path != cache_root:
        if contains_dataset(walking_path):
            return True
        walking_path = walking_path.parent
    return False


def clean_cache_dir(path):
    """Performs cleanup if something happens while creating an entry in the cache"""

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


def clean_cache():
    """Search the cache for directories that are not part of a dataset, and remove them.
    Also removes directories that do not have any datasets underneath them."""

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


def prune_cache_entry(sub_path):
    """Remove an entry from the cache by the given sub_path
    (a relative path, inside the cache)."""

    log.debug(f"Pruning cache entry '{sub_path}'")
    cache_root = config.get_cache_location()
    path = pathlib.Path(cache_root, sub_path)
    if not path.exists():
        log.info("Could not find entry in cache.")
        return

    log.info(f"Pruning Directory {path}")
    set_readwrite_recurse(path)
    shutil.rmtree(path, ignore_errors=True)
    clean_cache()


def schema_to_dict(schema: pyarrow.Schema) -> dict:
    """Convert a pyarrow.schema to a dict that can be serialized to JSON"""
    field_dict = {}
    for field in schema:
        field_dict[field.name] = str(field.type)
    return field_dict


def convert_arrow_alias(type_name: str) -> str:
    """Return the canonical name of the arrow type (or more precise: the name of the
    function to create it) in case the given type_name is an alias.
    Otherwise, return the type_name itself."""

    aliases = {
        "bool": "bool_",
        "halffloat": "float16",
        "float": "float32",
        "double": "float64",
        "decimal": "decimal128",
    }
    return aliases.get(type_name, type_name)


def arrow_type_function_lookup(function_name):
    """Return the function to create an instance of the pyarrow datatype with the given
    name"""
    if isinstance(function_name, str):
        function_name = convert_arrow_alias(function_name)
        pa_type_func = getattr(pa, function_name)
        return pa_type_func

    # The argument was not a pyarrow type (maybe a nested structure?)
    return None


def arrow_type_from_json(input_type):
    """Create and return an Arrow schema field for the given schema item
    from a JSON representation (string or dict)"""

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


def get_arrow_schema(input_schema: dict) -> pyarrow.Schema:
    """Convert the given schema, parsed from a JSON representation,
    to a pyarrow.schema"""

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


def generate_dataset(dataset):
    """Generate a dataset by calling one of the supported external generators"""

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
        generator = generator_class(executable_path=config.get_gen_location())

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
        dataset.write_metadata()

    except Exception:
        log.error("An error occurred during generation.")
        clean_cache_dir(dataset_path)
        raise

    return dataset


def compress(uncompressed_file_path, output_dir, compression):
    """Compress the given file or the files in given directory into given output
    directory, using given compression. The new file(s) will have a file extension
    based on the compression."""

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
    """Decompress the given file or the files in the given directory using given
    compression type. The last part of the file extension is removed to create the
    names for the new file(s), to these are assumes to have a compression suffix
    (e.g. .gz)."""

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
    """Download a given url to the fiven path. In case of a http url, this must be a
    single file. In case of S3, this can be a directory (bucket), that will be
    downloaded recursively. If the output path already exists, it will first be
    removed."""

    if output_path.exists():
        log.debug(f"Removing existing path '{output_path}'")
        shutil.rmtree(output_path, ignore_errors=True)

    try:
        if url[0:2] == "s3":
            # This performs a recursive download, so if the url points to a directory
            # it will download all the contents
            pyarrow.fs.copy_files(url, output_path)
        else:
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
