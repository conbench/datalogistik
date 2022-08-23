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

import pyarrow as pa
from pyarrow import csv as csv
from pyarrow import dataset as ds

from datalogistik import tpc_info

if len(sys.argv) != 2:
    print("Please provide a parameter for which TPC dataset to check")
    sys.exit(-1)

dataset = sys.argv[1]
encoding = "ISO-8859" if dataset == "tpc-ds" else "utf8"

failure_occurred = False
for table in tpc_info.tpc_table_names[dataset]:
    column_types = tpc_info.col_dicts[dataset][table]
    column_list = list(column_types.keys())

    # dbgen's .tbl output has a trailing delimiter
    column_types_trailed = column_types.copy()
    column_types_trailed["trailing_columns"] = pa.string()
    ro = csv.ReadOptions(column_names=column_types_trailed.keys(), encoding=encoding)
    po = csv.ParseOptions(delimiter="|")
    co = csv.ConvertOptions(column_types=column_types_trailed)
    dataset_read_format = ds.CsvFileFormat(
        read_options=ro, parse_options=po, convert_options=co
    )

    ref_ds = ds.dataset(
        f"./ref_data/{dataset}/0.001/{table}.tbl", format=dataset_read_format
    )
    ref_table = ref_ds.to_table(columns=list(column_types.keys()))
    gen_ds = ds.dataset(f"./tpc-h/{table}.parquet", format=ds.ParquetFileFormat())
    gen_table = gen_ds.to_table(columns=list(column_types.keys()))
    if ref_table == gen_table:
        print(f"Validation of table {table}: OK")
    else:
        print(f"Validation of table {table}: FAILED")
        failure_occurred = True
        print(f"ref ncols: {ref_table.num_columns} nrows: {ref_table.num_rows}")
        print(f"gen ncols: {gen_table.num_columns} nrows: {gen_table.num_rows}")

if failure_occurred:
    print("Error: validation failed!")
    sys.exit(-1)
