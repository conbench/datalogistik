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

import pyarrow as pa
from pyarrow import compute as pc
from pyarrow import csv as csv
from pyarrow import dataset as ds

from datalogistik import tpc_info


# Return a generator for rows of a given pyarrow table
def iter_patable_rows(table):
    for batch in table.to_batches():
        for row in zip(*batch.columns):
            yield row


def print_diff(ref_table, gen_table):
    for (i, (ref_row, gen_row)) in enumerate(
        zip(iter_patable_rows(ref_table), iter_patable_rows(gen_table))
    ):
        if ref_row != gen_row:
            print(f"row {i}: ")
            for (ref_val, gen_val) in zip(ref_row, gen_row):
                if ref_val != gen_val:
                    print(f"<ref:{ref_val}|gen:{gen_val}>", end=" ")
                else:
                    print(ref_val, end=" ")
            print("")


# Validate a TPC dataset.
# For TPC-H, it should have the same scale factor as the reference data (0.001)
# The TPC-DS reference data is meant to be used on valid test scale factors (e.g. 1000)
# Returns: True if valid, False if invalid
def validate_tpc_dataset(dataset_name, dataset_paths, file_format):
    if dataset_name == "tpc-ds":
        encoding = "ISO-8859"
        ext = "vld"
        ref_dataset_subpath = "tpc-ds"
    else:
        encoding = "utf8"
        ext = "tbl"
        ref_dataset_subpath = "tpc-h/0.001"

    failure_occurred = False
    for table, path in dataset_paths.items():
        column_types = tpc_info.col_dicts[dataset_name][table]
        column_list = list(column_types.keys())
        # dsdgen's validation output has a duplicated first column that we need to remove
        if dataset_name == "tpc-ds" and table not in [
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
        ro = csv.ReadOptions(
            column_names=column_types_trailed.keys(), encoding=encoding
        )
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
        gen_ds = ds.dataset(
            path,
            format=gen_dataset_read_format,
        )
        gen_table = gen_ds.to_table(columns=column_list)
        if dataset_name == "tpc-h":
            # resort, since order is not deterministic in pyarrow
            gen_table = gen_table.sort_by(
                [(x, "ascending") for x in gen_table.column_names]
            )
            ref_table = ref_table.sort_by(
                [(x, "ascending") for x in ref_table.column_names]
            )

            # Perform a simple equality check
            if ref_table == gen_table:
                print(f"Validation of table {table}: OK")
            else:
                print(f"Validation of table {table}: FAILED")
                failure_occurred = True
                print(f"ref ncols: {ref_table.num_columns} nrows: {ref_table.num_rows}")
                print(f"gen ncols: {gen_table.num_columns} nrows: {gen_table.num_rows}")
                print_diff(ref_table, gen_table)
        else:  # tpd-ds
            # For each row in the ref table, check if that row exists in the generated data
            missing_rows = []
            for row in iter_patable_rows(ref_table):
                # Build up an expression
                expr = pc.equal(gen_table[column_list[0]], row[0])
                for i in range(1, len(column_list)):
                    expr = pc.and_(expr, pc.equal(gen_table[column_list[i]], row[i]))
                result = pc.filter(gen_table, expr)
                if result.num_rows == 0:
                    failure_occurred = True
                    missing_rows.append(row)
            if len(missing_rows) != 0:
                print(
                    f"Validation of table {table}: FAILED with {len(missing_rows)} missing rows:"
                )
                print(missing_rows)

            else:
                print(f"Validation of table {table}: OK")

    return not failure_occurred
