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

from . import dataset_search, repo, util
from .dataset import Dataset
from .log import log


def parse_args():
    parser = argparse.ArgumentParser(
        prog=__file__,
        description="Dataset cacher/converter and generator",
    )
    sub_parsers = parser.add_subparsers(dest="command")
    sub_parsers.add_parser("clean")
    gen_parser = sub_parsers.add_parser("get")
    prune_parser = sub_parsers.add_parser("prune")
    val_parser = sub_parsers.add_parser("validate")
    val_parser.add_argument(
        "-a", "--all", action="store_true", help="Validate all entries"
    )
    val_parser.add_argument(
        "-r", "--remove", action="store_true", help="Remove entries failing validation"
    )

    for sub_parser in {gen_parser, val_parser, prune_parser}:
        sub_parser.add_argument(
            "-d",
            "--dataset",
            type=str,
            required=True if sub_parser in {gen_parser, prune_parser} else False,
            help="Name of the dataset to instantiate",
        )
        sub_parser.add_argument(
            "-f",
            "--format",
            type=str,
            required=True if sub_parser in {gen_parser, prune_parser} else False,
            help="Format for the dataset (convert if necessary). "
            "Supported formats: parquet, csv",
        )
        sub_parser.add_argument(
            "-c",
            "--compression",
            type=str,
            help="Compression (for parquet: passed to parquet writer, "
            "for csv: either None or gz)",
        )
        sub_parser.add_argument(
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

    return parser.parse_args()


def parse_args_and_get_dataset_info():
    # Parse and check cmdline options
    opts = parse_args()

    if opts.command == "clean":
        util.clean_cache()
        sys.exit(0)

    dataset = Dataset(
        name=opts.dataset,
        format=opts.format,
        scale_factor=opts.scale_factor,
        compression=opts.compression,
    )

    # Set defaults and perform sanity-check for the arguments:
    # TODO:
    #  * compression (in particular: test supported file-compression)
    #  * partitioning (later)
    dataset_from_repo = repo.search_repo(opts.dataset, repo.get_repo())
    if dataset_from_repo:
        dataset.fill_in_defaults(dataset_from_repo)
    if dataset.compression is None and dataset.format == "parquet":
        dataset.compression = "snappy"

    # add in partitioning to fake that it exists for now (since we don't want to expose it, but also don't want to rip up the code)
    opts.partition_max_rows = 0

    if opts.command == "get":
        return dataset

    entry = dataset_search.find_exact_dataset(dataset)
    if opts.command == "validate":
        if opts.all and (opts.dataset or opts.format or opts.compression):
            log.info(
                "Cannot combine --all flag with dataset name, format or compression options"
            )
            sys.exit(1)
        if opts.all:
            util.validate_cache(opts.remove)
        else:
            if entry:
                util.validate(entry.ensure_dataset_loc())
            else:
                log.info("Could not find entry in cache.")
        sys.exit(0)

    elif opts.command == "prune":
        if entry:
            util.prune_cache_entry(entry.ensure_dataset_loc())
        else:
            log.info("Could not find entry in cache.")
        sys.exit(0)

    else:
        msg = "Please specify a command"
        log.error(msg)
        raise RuntimeError(msg)
