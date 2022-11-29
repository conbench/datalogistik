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

import pathlib
from typing import List

import setuptools


def read_requirements_file(filepath: pathlib.Path) -> List[str]:
    """Parse a requirements.txt file into a list of package requirements"""
    print(filepath.resolve())
    with open(filepath, "r") as f:
        requirements = [
            line.strip()
            for line in f
            if line.strip() and not line.startswith("#") and not line.startswith("--")
        ]
    return requirements


repo_root = pathlib.Path(__file__).parent
print(repo_root)

__version__ = ""
with open(repo_root / "datalogistik" / "_version.py", "r") as f:
    exec(f.read())  # only overwrites the __version__ variable

with open(repo_root / "README.rst", "r") as f:
    long_description = f.read()

base_requirements = read_requirements_file(repo_root / "requirements.txt")
dev_requirements = read_requirements_file(repo_root / "requirements-dev.txt")

setuptools.setup(
    name="datalogistik",
    version=__version__,
    description="Dataset maintainer",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    packages=setuptools.find_packages(),
    entry_points={"console_scripts": ["datalogistik = datalogistik.datalogistik:main"]},
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: Apache 2 License",
    ],
    python_requires=">=3.8,<3.11",
    maintainer="Voltron Data",
    url="https://github.com/conbench/datalogistik",
    install_requires=base_requirements,
    extras_require={"dev": dev_requirements},
)
