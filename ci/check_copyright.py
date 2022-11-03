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

import argparse
import datetime
import fnmatch
import pathlib
import subprocess
from xml.etree import ElementTree

EXCLUSIONS = [
    "ref_data/*",
    "tests/fixtures/test_cache/*",
]


def file_is_excluded(file_path):
    return any([fnmatch.fnmatch(file_path, e) for e in EXCLUSIONS])


def check_copyright_headers(rat_jar: pathlib.Path):
    """Uses Apache Rat to check each file in this repo for a copyright and license
    information header.

    Does not check files in the EXCLUSIONS list. Needs java on the PATH.
    """
    repo_root = pathlib.Path(__file__).parent.parent.resolve()
    repo_name = repo_root.stem + "/"

    current_year = datetime.datetime.now().year
    year_string = "2022" if current_year == 2022 else f"2022-{current_year}"

    rat_result = subprocess.run(
        ["java", "-jar", rat_jar.resolve(), "--xml", repo_name],
        check=True,
        capture_output=True,
        text=True,
        cwd=repo_root.parent,
    )
    report = ElementTree.fromstring(rat_result.stdout)

    license_violations = []
    year_violations = []
    for file in report.findall("resource"):
        filename = file.attrib["name"]
        clean_filename = filename[len(repo_name) :]
        approval_elements = file.findall("license-approval")
        if not approval_elements or file_is_excluded(clean_filename):
            # this is a binary file or something else worth skipping
            continue
        elif approval_elements[0].attrib["name"] == "true":
            # license was approved; let's check the copyright year
            with open(repo_root / clean_filename, "r") as f:
                if year_string not in f.read():
                    year_violations.append(clean_filename)
        else:
            # license was not approved
            license_violations.append(clean_filename)

    assert not license_violations, f"Files failing Rat check: {license_violations}"
    assert not year_violations, f"Files without '{year_string}': {year_violations}"
    print("All files look great. :)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=check_copyright_headers.__doc__)
    parser.add_argument(
        "--rat-jar",
        help="Path to the Apache Rat jar file",
        type=pathlib.Path,
        required=True,
    )
    args = parser.parse_args()
    check_copyright_headers(rat_jar=args.rat_jar)
