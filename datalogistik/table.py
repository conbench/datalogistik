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

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Table:
    """A class that references a table (in a dataset).

    Parameters
    ----------
    """

    table: str
    # partitioning is not fully implemented and should not be exposed to the user
    partitioning: Optional[int] = None
    files: Optional[List] = field(default_factory=list)
    schema: Optional[dict] = None
    multi_file: Optional[bool] = None
    header_line: Optional[bool] = None
    dim: Optional[List] = field(default_factory=list)
    url: Optional[str] = None
    base_url: Optional[str] = None
