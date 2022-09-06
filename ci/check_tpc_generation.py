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

import pyarrow as pa
from pyarrow import compute as pc
from pyarrow import csv as csv
from pyarrow import dataset as ds

from datalogistik import config, tpc_info

if len(sys.argv) != 4:
    print(
        f"Usage: {sys.argv[1]} <tpc-h|tpc-ds> <csv|parquet> dataset_properties_file.json"
    )
    sys.exit(-1)

dataset = sys.argv[1]
if dataset not in tpc_info.tpc_datasets:
    print(f"Error, we only support datasets {tpc_info.tpc_datasets}")
file_format = sys.argv[2]
if file_format not in config.supported_formats:
    print(f"Error, we only support formats {config.supported_formats}")
if dataset == "tpc-ds":
    encoding = "ISO-8859"
    ext = "vld"
    ref_dataset_subpath = "tpc-ds"
    sf = "1"
else:
    encoding = "utf8"
    ext = "tbl"
    ref_dataset_subpath = "tpc-h/0.001"
    sf = "0.001"

with open(sys.argv[3]) as f:
    dataset_properties = json.load(f)
dataset_path = dataset_properties["path"]


def iter_patable_rows(table):
    for batch in table.to_batches():
        for row in zip(*batch.columns):
            yield row


failure_occurred = False
for table in tpc_info.tpc_table_names[dataset]:
    print(f"validating table {table}...")
    column_types = tpc_info.col_dicts[dataset][table]
    column_list = list(column_types.keys())
    # dsdgen's validation output has a duplicated first column that we need to remove
    if dataset == "tpc-ds" and table not in [
        "web_sales",
        "web_returns",
        # "time_dim", # has an additional column, but the data is not duplicate
        "store_sales",
        "store_returns",
        # "inventory", # has an additional column, but the data is not duplicate
        # "date_dim", # has an additional column, but the data is not duplicate
        "catalog_sales",
        "catalog_returns",
    ]:
        first_col_type = next(iter(column_types.values()))
        first_col_name = next(iter(column_types.keys()))
        new_dict = {}
        new_dict[first_col_name + "_dup"] = first_col_type
        new_dict.update(column_types)
        ref_column_types = new_dict
    else:
        ref_column_types = column_types

    # Both TPC generators' output have a trailing delimiter
    column_types_trailed = column_types.copy()
    column_types_trailed["trailing_columns"] = pa.string()
    ro = csv.ReadOptions(column_names=column_types_trailed.keys(), encoding=encoding)
    po = csv.ParseOptions(delimiter="|")
    co = csv.ConvertOptions(column_types=column_types_trailed)
    dataset_read_format = ds.CsvFileFormat(
        read_options=ro, parse_options=po, convert_options=co
    )
    if file_format == "csv":
        gen_dataset_read_format = dataset_read_format
    else:
        gen_dataset_read_format = ds.ParquetFileFormat()

    ref_column_types_trailed = ref_column_types.copy()
    ref_column_types_trailed["trailing_columns"] = pa.string()
    ro = csv.ReadOptions(
        column_names=ref_column_types_trailed.keys(), encoding=encoding
    )
    co = csv.ConvertOptions(column_types=ref_column_types_trailed)
    ref_dataset_read_format = ds.CsvFileFormat(
        read_options=ro, parse_options=po, convert_options=co
    )
    ref_ds = ds.dataset(
        f"./ref_data/{ref_dataset_subpath}/{table}.{ext}",
        format=ref_dataset_read_format,
    )
    ref_table = ref_ds.to_table(columns=column_list)
    ref_row_count = ref_table.num_rows
    gen_ds = ds.dataset(
        f"{dataset_path}/{table}.{file_format}",
        format=gen_dataset_read_format,
    )
    gen_table = gen_ds.to_table(columns=column_list)
    if dataset == "tpc-h":
        # Perform a simple equality check
        if ref_table == gen_table:
            print(f"Validation of table {table}: OK")
        else:
            print(f"Validation of table {table}: FAILED")
            failure_occurred = True
            print(f"ref ncols: {ref_table.num_columns} nrows: {ref_table.num_rows}")
            print(f"gen ncols: {gen_table.num_columns} nrows: {gen_table.num_rows}")
    else:  # tpd-ds
        # For each row in the ref table, check if that row exists in the generated data
        table_failures = 0
        for row in iter_patable_rows(ref_table):
            # Build up an expression
            expr = pc.equal(gen_table[column_list[0]], row[0])
            for i in range(1, len(column_list)):
                expr = pc.and_(expr, pc.equal(gen_table[column_list[i]], row[i]))
            result = pc.filter(gen_table, expr)
            if result.num_rows != 0:
                failure_occurred = True
                table_failures += 1
        if table_failures != 0:
            print(f"Validation of table {table}: FAILED with {table_failures} failures")
        else:
            print(f"Validation of table {table}: OK")

if failure_occurred:
    print("Error: validation failed!")
    sys.exit(-1)
else:
    print("Validation passed.")
