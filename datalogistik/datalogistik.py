import os
import pathlib
import sys
import time

from . import cli
from . import config
from . import tpc_info
from . import util

total_start = time.perf_counter()


def finish():
    total_time = time.perf_counter() - total_start
    print("Done.")
    util.debug_print(f"Full process took {total_time:0.2f} s")
    sys.exit(0)


local_cache_location = pathlib.Path(
    os.getenv("DATALOGISTIK_CACHE", config.default_cache_location),
    config.cache_dir_name,
)


def main():
    (dataset_info, argument_info) = cli.parse_args_and_get_dataset_info()
    print(
        f"Creating an instance of Dataset '{argument_info.dataset}' in '{argument_info.format}' format..."
    )
    # Here could be a check for whether this dataset instance already exists.
    # However, because it may be possible that it was generated using different parameters
    # we skip it for now. It will be in the cache so the penalty for re-creating it
    # is small. In the future, we could inquire the metadata file to check if an existing
    # dataset is still valid.

    util.debug_print(f"Checking local cache at {local_cache_location}")
    cached_dataset_path = pathlib.Path(
        local_cache_location,
        argument_info.dataset,
        argument_info.scale_factor,
        argument_info.format,
        str(argument_info.partition_max_rows),
    )
    cached_dataset_metadata_file = pathlib.Path(
        cached_dataset_path, config.metadata_filename
    )
    if cached_dataset_metadata_file.exists():
        util.debug_print(
            f"Found cached dataset metadata file at '{cached_dataset_metadata_file}'"
        )
        util.copy_from_cache(cached_dataset_path, argument_info.dataset)
        finish()
    else:  # not found in cache, check if the cache has other formats of this dataset
        util.debug_print(
            f"No cached data metadata file found at '{cached_dataset_metadata_file}'"
        )
        for cached_file_format in [x for x in config.supported_formats]:
            other_format_path = pathlib.Path(
                local_cache_location,
                argument_info.dataset,
                argument_info.scale_factor,
                cached_file_format,
            )
            if other_format_path.exists():
                util.debug_print(
                    f"Found cached instance(s) with a different format/partitioning at '{other_format_path}'"
                )
                # Find a partitioning (any, really, we're going to convert it anyway)
                subfolders = [
                    f.name for f in os.scandir(other_format_path) if f.is_dir()
                ]
                for cached_nrows in subfolders:
                    other_nrows_path = pathlib.Path(other_format_path, cached_nrows)
                    cached_dataset_metadata_file = pathlib.Path(
                        other_nrows_path, config.metadata_filename
                    )
                    if cached_dataset_metadata_file.exists():
                        util.debug_print(
                            f"Found cached dataset in different format/partitioning at '{other_nrows_path}'"
                        )
                        util.debug_print(
                            f"Metadata file located at '{cached_dataset_metadata_file}'"
                        )
                        cached_dataset_path = util.convert_dataset(
                            local_cache_location,
                            dataset_info,
                            argument_info.compression,
                            cached_file_format,
                            argument_info.format,
                            cached_nrows,
                            argument_info.partition_max_rows,
                        )
                        util.copy_from_cache(cached_dataset_path, argument_info.dataset)
                        finish()
                    else:
                        print("Found cached dataset without metadata file, cleaning...")
                        util.clean_cache_dir(other_nrows_path)

    # If we have not exited at this point, nothing useable was found in the local cache.
    # We need to either generate or download the data.
    print("Dataset not found in local cache.")

    if argument_info.dataset in tpc_info.tpc_datasets:
        cached_dataset_path = util.generate_dataset(
            dataset_info, argument_info, local_cache_location
        )
    else:
        cached_dataset_path = util.download_dataset(
            dataset_info, argument_info, local_cache_location
        )

    # Convert to the requested format if necessary
    if (dataset_info["format"] != argument_info.format) or (
        dataset_info["partitioning-nrows"] != argument_info.partition_max_rows
    ):
        cached_dataset_path = util.convert_dataset(
            local_cache_location,
            dataset_info,
            argument_info.compression,
            dataset_info["format"],
            argument_info.format,
            dataset_info["partitioning-nrows"],
            argument_info.partition_max_rows,
        )

    # Copy to the actual output location
    util.copy_from_cache(cached_dataset_path, argument_info.dataset)
    finish()


if __name__ == "__main__":
    main()
