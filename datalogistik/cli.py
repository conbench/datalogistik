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

import argparse
import sys

from . import util
from .dataset import Dataset
from .dataset_search import find_exact_dataset
from .log import log


def parse_args():
    parser = argparse.ArgumentParser(
        prog=__file__,
        description="Dataset cacher/converter and generator",
    )
    sub_parsers = parser.add_subparsers(dest="command")
    cache_parser = sub_parsers.add_parser("cache")
    gen_parser = sub_parsers.add_parser("get")
    meta_parser = sub_parsers.add_parser("metadata")

    cache_group = cache_parser.add_mutually_exclusive_group()
    cache_group.add_argument(
        "--prune-entry",
        type=str,
        default=None,
        help="Remove entry or entries, specified by their relative path, from the cache",
    )
    cache_group.add_argument(
        "--prune-invalid",
        type=str,
        default=None,
        help="Validate all entries in the cache for file integrity and remove entries that fail",
    )
    cache_group.add_argument(
        "--clean",
        action="store_true",
        help="Remove any incomplete/left-over directories from the cache",
    )
    cache_group.add_argument(
        "--validate",
        action="store_true",
        help="Validate all entries in the cache for file integrity and report entries that fail",
    )

    gen_parser.add_argument(
        "-d",
        "--dataset",
        type=str,
        required=True,
        help="Name of the dataset to instantiate",
    )
    gen_parser.add_argument(
        "-f",
        "--format",
        type=str,
        required=True,
        help="Format for the dataset (convert if necessary). "
        "Supported formats: parquet, csv",
    )
    gen_parser.add_argument(
        "-c",
        "--compression",
        type=str,
        help="Compression (for parquet: passed to parquet writer, "
        "for csv: either None or gz)",
    )
    gen_parser.add_argument(
        "-s",
        "--scale-factor",
        type=str,
        default=None,
        help="Scale factor for TPC datasets",
    )
    # TODO: should this be an env var instead of an argument?
    gen_parser.add_argument(
        "-g",
        "--generator-path",
        type=str,
        default=None,
        help="Path to the location of the external generator (e.g. TPC-H's dbgen). If "
        "not given, datalogistik will attempt to make it by cloning a repo (requires "
        "git on your PATH) and building the tool (requires make for UNIX or msbuild "
        "for Windows on your PATH).",
    )

    meta_parser.add_argument(
        "-d",
        "--dataset",
        type=str,
        required=True,
        help="Name of the dataset",
    )
    meta_parser.add_argument(
        "-f",
        "--format",
        type=str,
        required=True,
        help="Format for the dataset. \
Supported formats: Parquet, csv",
    )
    meta_parser.add_argument(
        "-c",
        "--compression",
        type=str,
        help="Compression (for parquet: passed to parquet writer, "
        "for csv: either None or gz)",
    )
    meta_parser.add_argument(
        "-s",
        "--scale-factor",
        type=str,
        default=None,
        help="Scale factor for TPC datasets",
    )

    return parser.parse_args()


def handle_cache_command(cache_opts):
    if cache_opts.prune_entry:
        util.prune_cache_entry(cache_opts.prune_entry)
    elif cache_opts.clean:
        util.clean_cache()
    elif cache_opts.validate:
        util.validate_cache(False)
    elif cache_opts.prune_invalid:
        util.validate_cache(True)
    else:
        msg = "Please specify a cache-specific option"
        log.error(msg)
        raise RuntimeError(msg)


def handle_metadata_command(opts):
    dataset = Dataset(
        name=opts.dataset,
        format=opts.format,
        scale_factor=opts.scale_factor,
        compression=opts.compression,
    )
    # Get dataset if it already exists in the cache
    exact_match = find_exact_dataset(dataset)

    if exact_match:
        with open(exact_match.metadata_file) as metadata_file:
            print(metadata_file.read())
    else:
        log.info(
            "Could not find a corresponding dataset in the cache. \n"
            "Make sure this dataset is instantiated by running the 'get' command first."
        )


def parse_args_and_get_dataset_info():
    # Parse and check cmdline options
    opts = parse_args()

    # add in partitioning to fake that it exists for now (since we don't want to expose it, but also don't want to rip up the code)
    opts.partition_max_rows = 0

    if opts.command == "cache":
        handle_cache_command(opts)
        sys.exit(0)

    elif opts.command == "metadata":
        handle_metadata_command(opts)
        sys.exit(0)

    elif opts.command == "get":
        dataset = Dataset(
            name=opts.dataset,
            format=opts.format,
            scale_factor=opts.scale_factor,
            compression=opts.compression,
        )

        # Set defaults and perform sanity-check for the arguments:
        # TODO:
        #  * format
        #  * scale_factor
        #  * compression
        #  * partitioning (later)

        return dataset

    else:
        msg = "Please specify a command"
        log.error(msg)
        raise RuntimeError(msg)
