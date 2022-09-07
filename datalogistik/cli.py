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
import json
import sys

import urllib3

from . import config, tpc_info, util
from .log import log


def parse_args():
    parser = argparse.ArgumentParser(
        prog=__file__,
        description="Dataset cacher/converter and generator",
    )
    sub_parsers = parser.add_subparsers(dest="command")
    cache_parser = sub_parsers.add_parser("cache")
    gen_parser = sub_parsers.add_parser("generate")

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
        help="Format for the dataset (convert if necessary). \
Supported formats: Parquet, csv",
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
        default="",
        help="Scale factor for TPC datasets",
    )
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
    gen_parser.add_argument(
        "-p",
        "--partition-max-rows",
        type=int,
        default=0,
        help="Partition the dataset using this maximum number of rows per file",
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


def parse_args_and_get_dataset_info():
    # Parse and check cmdline options
    opts = parse_args()
    if opts.command == "cache":
        handle_cache_command(opts)
        sys.exit(0)

    elif opts.command == "generate":
        # Set up repository (local or remote)
        repo_location = config.get_repo_file_path()
        if repo_location[0:4] == "http":
            log.debug(f"Fetching repo from {repo_location}")
            try:
                http = urllib3.PoolManager()
                r = http.request("GET", repo_location)
                dataset_sources = json.loads(r.data.decode("utf-8"))
            except Exception:
                log.error(f"Unable to download from '{repo_location}'")
                raise
        else:
            log.debug(f"Using local repo at {repo_location}")
            dataset_sources = json.load(open(repo_location))

        # Find requested dataset in repository, then in the list of generators
        dataset_info = None
        for dataset_source in dataset_sources:
            if dataset_source["name"] == opts.dataset:
                dataset_info = dataset_source
                break
        if dataset_info is None and opts.dataset not in tpc_info.tpc_datasets:
            msg = (
                f"Dataset '{opts.dataset}' not found in repository or list of supported "
                "generators.\n\nDatasets found in repository: "
                f"{[source['name'] for source in dataset_sources]}\nSupported generators: "
                f"{tpc_info.tpc_datasets}"
            )
            log.error(msg)
            raise ValueError(msg)

        # Defaults
        if opts.compression is None:
            if opts.format == "parquet":
                opts.compression = "snappy"
            if opts.format == "csv":
                opts.compression = "gz"

        # Note the difference between None and a string "None"
        if (
            opts.compression.lower() == "uncompressed"
            or opts.compression.lower() == "none"
        ):
            opts.compression = None

        if opts.scale_factor != "" and opts.dataset not in tpc_info.tpc_datasets:
            msg = "scale-factor is only supported for TPC datasets"
            log.error(msg)
            raise ValueError(msg)
        if opts.scale_factor == "" and opts.dataset in tpc_info.tpc_datasets:
            opts.scale_factor = "1"

        if opts.dataset in tpc_info.tpc_datasets:
            # Construct an dataset_info for a generated dataset
            dataset_info = {
                "name": opts.dataset,
                "format": "csv",
                "delim": "|",
                "scale-factor": opts.scale_factor,
                "partitioning-nrows": opts.partition_max_rows,
            }

        if opts.format not in config.supported_formats:
            msg = (
                f"Format '{opts.format}' not supported. Supported formats: "
                f"{config.supported_formats}"
            )
            log.error(msg)
            raise ValueError(msg)

        if "partitioning-nrows" not in dataset_info:
            dataset_info["partitioning-nrows"] = 0  # Default: no partitioning

        return (dataset_info, opts)

    else:
        msg = "Please specify a command"
        log.error(msg)
        raise RuntimeError(msg)
