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

import os
import pathlib
import sys
import time

import pyarrow

from . import cli, config, tpc_info, util
from .log import log

total_start = time.perf_counter()


def finish():
    total_time = time.perf_counter() - total_start
    log.info("Done.")
    log.debug(f"Full process took {total_time:0.2f} s")
    sys.exit(0)


def main():
    (dataset_info, argument_info) = cli.parse_args_and_get_dataset_info()
    if config.get_max_cpu_count() != 0:
        pyarrow.set_cpu_count(config.get_max_cpu_count())
        pyarrow.set_io_thread_count(config.get_max_cpu_count())
    log.info(
        f"Creating an instance of Dataset '{argument_info.dataset}' in "
        f"'{argument_info.format}' format..."
    )

    local_cache_location = config.get_cache_location()
    log.debug(f"Checking local cache at {local_cache_location}")
    cached_dataset_path = util.create_cached_dataset_path(
        argument_info.dataset,
        argument_info.scale_factor,
        argument_info.format,
        str(argument_info.partition_max_rows),
        argument_info.compression,
    )
    cached_dataset_metadata_file = pathlib.Path(
        cached_dataset_path, config.metadata_filename
    )
    if cached_dataset_metadata_file.exists():
        log.debug(f"Found cached dataset at '{cached_dataset_metadata_file}'")
        util.output_result(cached_dataset_path)
        finish()

    # Do not convert a tpc dataset to CSV, because Arrow cannot cast decimals to string (ARROW-17458)
    elif not (
        argument_info.dataset in tpc_info.tpc_datasets and argument_info.format == "csv"
    ):
        # not found in cache, check if the cache has other formats of this dataset
        for cached_file_format in [x for x in config.supported_formats]:
            if argument_info.scale_factor:
                scale_factor = f"scalefactor_{argument_info.scale_factor}"
            else:
                scale_factor = ""

            other_format_path = pathlib.Path(
                local_cache_location,
                argument_info.dataset,
                scale_factor,
                cached_file_format,
            )
            if other_format_path.exists():
                log.debug(
                    "Found cached instance(s) with a different format/partitioning/compression at "
                    f"'{other_format_path}'"
                )
                format_subfolders = [
                    f.name for f in os.scandir(other_format_path) if f.is_dir()
                ]
                # Grab the first subdir, we're going to convert it anyway
                cached_nrows_dir = format_subfolders[0]
                similar_dataset_path = other_format_path / cached_nrows_dir
                cached_nrows = int(cached_nrows_dir.split("_")[-1])
                nrows_subfolders = [
                    f.name for f in os.scandir(similar_dataset_path) if f.is_dir()
                ]
                cached_compression_dir = nrows_subfolders[0]
                similar_dataset_path = similar_dataset_path / cached_compression_dir
                cached_compression = cached_compression_dir.split("_")[-1]

                cached_dataset_metadata_file = pathlib.Path(
                    similar_dataset_path, config.metadata_filename
                )
                if cached_dataset_metadata_file.exists():
                    log.debug(
                        "Found cached dataset in different format/partitioning/compression at "
                        f"'{similar_dataset_path}'"
                    )
                    cached_dataset_path = util.convert_dataset(
                        dataset_info,
                        cached_compression,
                        argument_info.compression,
                        cached_file_format,
                        argument_info.format,
                        cached_nrows,
                        argument_info.partition_max_rows,
                    )
                    util.output_result(cached_dataset_path)
                    finish()
                else:
                    log.info("Found cached dataset without metadata file, cleaning...")
                    util.clean_cache_dir(similar_dataset_path)

    # If we have not exited at this point, nothing useable was found in the local cache.
    # We need to either generate or download the data.
    log.info("Dataset not found in local cache.")

    if argument_info.dataset in tpc_info.tpc_datasets:
        cached_dataset_path = util.generate_dataset(dataset_info, argument_info)
    else:
        cached_dataset_path = util.download_dataset(dataset_info)

    if dataset_info["format"] == "parquet":
        # retrieve the compression from the directory name
        compression = cached_dataset_path.parts[-1].split("_")[-1].lower()
    else:
        compression = dataset_info.get("file-compression")
    # Convert if necessary
    if (dataset_info["format"] != argument_info.format) or (
        dataset_info["partitioning-nrows"] != argument_info.partition_max_rows
        or (compression != argument_info.compression)
    ):
        cached_dataset_path = util.convert_dataset(
            dataset_info,
            compression,
            argument_info.compression,
            dataset_info["format"],
            argument_info.format,
            dataset_info["partitioning-nrows"],
            argument_info.partition_max_rows,
        )

    util.output_result(cached_dataset_path)
    finish()


if __name__ == "__main__":
    main()
