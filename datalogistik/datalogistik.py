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

import sys
import time

import pyarrow

from . import cli, config, dataset_search, repo
from .log import log

total_start = time.perf_counter()


def finish():
    total_time = time.perf_counter() - total_start
    log.info("Done.")
    log.debug(f"Full process took {total_time:0.2f} s")
    sys.exit(0)


def main(dataset=None):
    # dataset here should typically be None, so then we use parse_args_and_get_dataset_info() to
    # create the dataset to use. But it can be helpful in tests to construct ones own dataset
    # with Dataset(name="my dataset", format="very_fancy") and pass it as the dataset argument
    if dataset is None:
        dataset = cli.parse_args_and_get_dataset_info()

    if config.get_max_cpu_count() != 0:
        pyarrow.set_cpu_count(config.get_max_cpu_count())
        pyarrow.set_io_thread_count(config.get_max_cpu_count())

    if dataset.remote:
        matching_dataset = repo.search_repo(dataset.name, repo.get_repo())
        if matching_dataset is None:
            msg = (
                f"Dataset '{dataset.name}' not found in repository."
                f"\n\nDatasets found in repository: "
                f"{[source.name for source in repo.get_repo()]}"
            )
            log.error(msg)
            raise ValueError(msg)
        elif matching_dataset != dataset:
            msg = (
                f"Dataset '{dataset.name}' found in repository"
                "has different properties from those requested by the user, "
                "but conversions are not supported for remote datasets.\n"
            )
            log.error(msg)
            raise ValueError(msg)
        else:
            matching_dataset.remote = True
            print(matching_dataset.output_result())
            finish()

    log.info(
        f"Creating an instance of Dataset '{dataset.name}' in "
        f"'{dataset.format}' format..."
    )

    # Get dataset if it already exists in the cache
    exact_match = dataset_search.find_exact_dataset(dataset)

    if exact_match:
        print(exact_match.output_result())
        finish()

    # Convert if not
    close_match = dataset_search.find_or_instantiate_close_dataset(dataset)
    if close_match != dataset:
        new_dataset = close_match.convert(dataset)
    else:
        # but if the downloaded dataset is an exact match, we print it
        new_dataset = close_match
    print(new_dataset.output_result())

    finish()


if __name__ == "__main__":
    main()
