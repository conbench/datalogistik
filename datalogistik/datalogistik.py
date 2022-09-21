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

from . import cli, config, dataset_search
from .log import log

total_start = time.perf_counter()


def finish():
    total_time = time.perf_counter() - total_start
    log.info("Done.")
    log.debug(f"Full process took {total_time:0.2f} s")
    sys.exit(0)


def main(dataset=None):
    if dataset is None:
        dataset = cli.parse_args_and_get_dataset_info()

    if config.get_max_cpu_count() != 0:
        pyarrow.set_cpu_count(config.get_max_cpu_count())
        pyarrow.set_io_thread_count(config.get_max_cpu_count())
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
        print(new_dataset.output_result())

    finish()


if __name__ == "__main__":
    main()
