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
import os

import urllib3

from . import config, tpc_info
from .log import log


def parse_args():
    parser = argparse.ArgumentParser(
        prog=__file__,
        description="Dataset cacher/converter and generator",
    )

    parser.add_argument(
        "-d",
        "--dataset",
        type=str,
        required=True,
        help="Name of the dataset to instantiate",
    )
    parser.add_argument(
        "-f",
        "--format",
        type=str,
        required=True,
        help="Format for the dataset (convert if necessary). \
Supported formats: Parquet, csv",
    )
    parser.add_argument(
        "-c",
        "--compression",
        type=str,
        help="Internal compression (passed to parquet writer)",
    )
    parser.add_argument(
        "-s",
        "--scale-factor",
        type=str,
        default="",
        help="Scale factor for TPC datasets",
    )
    parser.add_argument(
        "-g",
        "--generator-path",
        type=str,
        default=None,
        help="Path to the location of the external generator (e.g. TPC-H's dbgen). If "
        "not given, datalogistik will attempt to make it by cloning a repo (requires "
        "git on your PATH) and building the tool (requires make for UNIX or msbuild "
        "for Windows on your PATH).",
    )
    parser.add_argument(
        "-p",
        "--partition-max-rows",
        type=int,
        default=0,
        help="Partition the dataset using this maximum number of rows per file",
    )
    parser.add_argument(
        "-b",
        "--bypass-cache",
        action="store_true",
        help="Do not store any copies of the dataset in the cache",
    )

    return parser.parse_args()


def parse_args_and_get_dataset_info():
    # Set up repository (local or remote)
    repo_location = os.getenv("DATALOGISTIK_REPO", config.default_repo_file)
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

    # Parse and check cmdline options
    opts = parse_args()

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

    if opts.compression is not None:
        if opts.format != "parquet":
            msg = "Compression is only supported for parquet format"
            log.error(msg)
            raise ValueError(msg)

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
            "partitioning-nrows": 0,
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
