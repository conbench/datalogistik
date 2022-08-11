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
import shutil
import sys
import time

from . import cli, config, tpc_info, util
from .log import log

total_start = time.perf_counter()


def finish():
    total_time = time.perf_counter() - total_start
    log.info("Done.")
    log.debug(f"Full process took {total_time:0.2f} s")
    sys.exit(0)


local_cache_location = config.get_cache_location()


def main():
    (dataset_info, argument_info) = cli.parse_args_and_get_dataset_info()
    log.info(
        f"Creating an instance of Dataset '{argument_info.dataset}' in "
        f"'{argument_info.format}' format..."
    )
    # Here could be a check for whether this dataset instance already exists.
    # However, because it may be possible that it was generated using different
    # parameters, we skip it for now. It will be in the cache so the penalty for
    # re-creating it is small. In the future, we could inquire the metadata file to
    # check if an existing dataset is still valid.

    log.debug(f"Checking local cache at {local_cache_location}")
    cached_dataset_path = util.create_cached_dataset_path(
        argument_info.dataset,
        argument_info.scale_factor,
        argument_info.format,
        str(argument_info.partition_max_rows),
    )
    cached_dataset_metadata_file = pathlib.Path(
        cached_dataset_path, config.metadata_filename
    )
    if cached_dataset_metadata_file.exists():
        log.debug(
            f"Found cached dataset metadata file at '{cached_dataset_metadata_file}'"
        )
        util.copy_from_cache(cached_dataset_path, argument_info.dataset)
        finish()
    else:  # not found in cache, check if the cache has other formats of this dataset
        log.debug(
            f"No cached data metadata file found at '{cached_dataset_metadata_file}'"
        )
        for cached_file_format in [x for x in config.supported_formats]:
            other_format_path = pathlib.Path(
                local_cache_location,
                argument_info.dataset,
                f"scalefactor_{argument_info.scale_factor}",
                cached_file_format,
            )
            if other_format_path.exists():
                log.debug(
                    "Found cached instance(s) with a different format/partitioning at "
                    f"'{other_format_path}'"
                )
                # Find a partitioning (any, really, we're going to convert it anyway)
                subfolders = [
                    f.name for f in os.scandir(other_format_path) if f.is_dir()
                ]
                for cached_nrows in subfolders:
                    other_nrows_path = pathlib.Path(other_format_path, cached_nrows)
                    cached_nrows = cached_nrows.split("_")[-1]
                    cached_dataset_metadata_file = pathlib.Path(
                        other_nrows_path, config.metadata_filename
                    )
                    if cached_dataset_metadata_file.exists():
                        log.debug(
                            "Found cached dataset in different format/partitioning at "
                            f"'{other_nrows_path}'"
                        )
                        log.debug(
                            f"Metadata file located at '{cached_dataset_metadata_file}'"
                        )
                        cached_dataset_path = util.convert_dataset(
                            dataset_info,
                            argument_info.compression,
                            cached_file_format,
                            argument_info.format,
                            cached_nrows,
                            argument_info.partition_max_rows,
                        )
                        util.copy_from_cache(cached_dataset_path, argument_info.dataset)
                        if argument_info.bypass_cache:
                            log.info("Removing cache entry")
                            shutil.rmtree(cached_dataset_path, ignore_errors=True)
                            util.clean_cache_dir(cached_dataset_path)
                        finish()
                    else:
                        log.info(
                            "Found cached dataset without metadata file, cleaning..."
                        )
                        util.clean_cache_dir(other_nrows_path)

    # If we have not exited at this point, nothing useable was found in the local cache.
    # We need to either generate or download the data.
    log.info("Dataset not found in local cache.")

    if argument_info.dataset in tpc_info.tpc_datasets:
        cached_dataset_path = util.generate_dataset(dataset_info, argument_info)
    else:
        cached_dataset_path = util.download_dataset(dataset_info, argument_info)

    # Convert to the requested format if necessary
    if (dataset_info["format"] != argument_info.format) or (
        dataset_info["partitioning-nrows"] != argument_info.partition_max_rows
    ):
        cached_dataset_path = util.convert_dataset(
            dataset_info,
            argument_info.compression,
            dataset_info["format"],
            argument_info.format,
            dataset_info["partitioning-nrows"],
            argument_info.partition_max_rows,
        )

    # Copy to the actual output location
    util.copy_from_cache(cached_dataset_path, argument_info.dataset)
    if argument_info.bypass_cache:
        log.info("Removing cache entry")
        shutil.rmtree(cached_dataset_path, ignore_errors=True)
        util.clean_cache_dir(cached_dataset_path)
    finish()


if __name__ == "__main__":
    main()
