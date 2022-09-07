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

import json
import sys

from datalogistik import config, tpc_info, tpc_validation

if len(sys.argv) != 2:
    print(f"Usage: {sys.argv[0]} dataset_properties_file.json")
    sys.exit(-1)


with open(sys.argv[1]) as f:
    dataset_properties = json.load(f)
path = dataset_properties["path"]
dataset = dataset_properties["name"]
file_format = dataset_properties["format"]

if dataset not in tpc_info.tpc_datasets:
    print(f"Error, this script can only validate datasets {tpc_info.tpc_datasets}")
if file_format not in config.supported_formats:
    print(f"Error, this script only support formats {config.supported_formats}")

if tpc_validation.validate_tpc_dataset(dataset, path, file_format):
    print("Validation passed.")
else:
    print("Error: validation failed!")
    sys.exit(-1)
