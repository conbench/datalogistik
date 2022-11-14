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

import time

from . import config, tpc_info
from .dataset import Dataset
from .log import log
from .table import Table
from .tpc_builders import DBGen, DSDGen
from .util import clean_cache_dir


# TODO: Generator output cannot be used as dataset output directly, because of the
# trailing columns.
def generate_dataset(dataset: Dataset) -> None:
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
