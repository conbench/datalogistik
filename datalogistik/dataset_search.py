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

import copy

from . import Dataset, config, repo, tpc_info, util
from .log import log


def find_exact_dataset(dataset):
    variants = dataset.list_variants()
    # find exact
    try:
        index = variants.index(dataset)
        return variants[index]
    except ValueError:
        return None


# Will also download if no close variant is found (is this right? should this happen higher up?)
def find_or_instantiate_close_dataset(dataset):
    # make a copy so we don't alter the dataset passed
    dataset = copy.deepcopy(dataset)
    variants = dataset.list_variants()

    # If no variants are available, ensure that one is
    if variants == []:
        # N. B. this will raise an error for unrecognized datasets
        dataset_to_fetch = repo.search_repo(dataset.name, repo.get_repo())
        if dataset_to_fetch:
            # we found a dataset, so we can use it
            dataset_to_fetch.download()
            # Read in the JSON after downloading, because it could contain more metadata
            # that was detected from the file(s), like format and compression
            variants = [
                Dataset.from_json(
                    dataset_to_fetch.ensure_dataset_loc() / config.metadata_filename
                )
            ]

    if dataset.name in tpc_info.tpc_datasets:
        # filter variants to the same scale factor (and all tpc datasets require scale factor...)
        variants = [x for x in variants if x.scale_factor == dataset.scale_factor]
        if variants == []:
            # this is generatable + must be generated
            variants = [util.generate_dataset(dataset)]

    if variants == []:
        msg = (
            f"Dataset '{dataset.name}' not found in repository or list of supported "
            "generators.\n\nDatasets found in repository: "
            f"{[source.name for source in repo.get_repo()]}\nSupported generators: "
            f"{tpc_info.tpc_datasets}"
        )
        log.error(msg)
        raise ValueError(msg)

    # order parquet first (since they should be fast(er) to convert from)
    # Try Arrow first, as it has schema info and performs well, then parquet, et c.
    # TODO: sort by compression too?
    format_preference = {
        "arrow": 0,
        "parquet": 1,
        "csv": 2,
        "tpc-raw": 3,
    }
    variants.sort(key=lambda c: format_preference[c.format])

    return variants[0]
