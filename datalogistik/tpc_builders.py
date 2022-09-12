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

import abc
import concurrent.futures
import os
import pathlib
import platform
import shutil
import subprocess
from typing import List, Optional

from .config import get_thread_count
from .log import log
from .tpc_info import tpc_table_names

local_package_root = pathlib.Path(__file__).parent.resolve()


def _run(*args, **kwargs) -> str:
    """Run a command in the shell, checking its return code and returning output.

    Parameters
    ----------
    *args
        Command to execute
    **kwargs
        Keyword args to subprocess.run()
    """
    log.debug(f"Calling {args}")
    try:
        res = subprocess.run(
            args,
            **kwargs,
            check=True,
            capture_output=True,
            text=True,
        )
        stdout = res.stdout.strip()
        stderr = res.stderr.strip()
        log.debug(f"Done calling {args}.\nstdout:\n{stdout}\nstderr:\n{stderr}\n<end>")
        return stdout
    except subprocess.CalledProcessError as e:
        log.error("An exception was raised during a subprocess. Details:")
        for key, value in e.__dict__.items():
            log.error(f"{key}:\n    {value}")
        raise


class _TPCBuilder(abc.ABC):
    """A class to help with creating and calling a TPC database generator.

    Parameters
    ----------
    executable_path
        Path to the generator executable for this TPC database.
    """

    executable_path: Optional[pathlib.Path] = None
    system: str = None

    # details about the generator
    force_flag: str
    scale_flag: str
    partitions_flag: str
    current_segment_flag: str
    file_extension: str
    table_names: List[str]

    # details about the repo to clone if necessary
    repo_uri: str
    repo_commit: str
    repo_local_path: pathlib.Path
    repo_build_path: pathlib.Path

    def __init__(self, executable_path: Optional[pathlib.Path]):
        self.system = platform.system()

        if executable_path:
            executable_path = pathlib.Path(executable_path).resolve()
            if not executable_path.exists():
                msg = f"The given executable_path '{executable_path}' does not exist."
                log.error(msg)
                raise ValueError(msg)
            self.executable_path = executable_path
        else:
            self.executable_path = self._get_default_executable_path()

    @abc.abstractmethod
    def _get_default_executable_path(self):
        """Return the executable path for this generator if one isn't given."""

    @abc.abstractmethod
    def _get_partitioned_filename(self, table_name: str, p: int, partitions: int):
        """Create the pathname for a file in case of a partitioned output"""

    @abc.abstractmethod
    def _get_parallel_table_name_flags(self):
        """Create a list of the flags needed to generate all the individual tables
        that can be generated in parallel"""

    @abc.abstractmethod
    def _get_serial_table_name_flags(self):
        """Create a list of the flags needed to generate all the individual tables
        that cannot be generated in parallel and should be generated in sequence"""

    def create_dataset(
        self,
        out_dir: pathlib.Path,
        scale_factor: float = 1,
        partitions: int = 1,
    ):
        """Call the executable to generate the TPC database.

        Parameters
        ----------
        out_dir
            The directory to place the CSVs in.
        scale_factor
            The scale factor to use when building the database. Default 1.
        partitions
            The number of partitions to create, these will be generated in parallel.
            Default 1.
        """
        if not self.executable_path or not self.executable_path.exists():
            log.info("Could not find an executable. Attempting to create one.")
            self._make_executable()

        partitions = 1 if partitions == 0 else partitions
        num_cpus = get_thread_count()

        # TODO: merge partitions and num_cpus, right now they should be the same thing
        with concurrent.futures.ProcessPoolExecutor(num_cpus) as pool:
            futures = []
            for p in range(1, partitions + 1):
                futures.append(
                    pool.submit(
                        _run,
                        self.executable_path,
                        self.force_flag,
                        self.scale_flag,
                        str(scale_factor),
                        self.partitions_flag,
                        str(partitions),
                        self.current_segment_flag,
                        str(p),
                        cwd=self.executable_path.parent,
                    )
                )

        # Move the new files to out_dir
        out_dir = pathlib.Path(out_dir).resolve()
        for table_name in self.table_names:
            table_output_dir = out_dir / f"{table_name}"
            if not table_output_dir.exists():
                table_output_dir.mkdir()
            for p in range(1, partitions + 1):
                old_file = self.executable_path.parent / self._get_partitioned_filename(
                    table_name, p, partitions
                )
                new_file = table_output_dir / f"part-{p}.tpc-raw"
                # Not all tables will have enough rows for the full nr of partitions
                if old_file.exists():
                    shutil.move(old_file, new_file)
                    # reset permissions to read-only
                    os.chmod(new_file, 0o444)
                    log.debug(f"Created {new_file}")
            os.chmod(table_output_dir, 0o555)
            log.debug(f"Created all partitions for {table_name}")

    def _make_executable(self):
        """Clone the repo and build the executable."""
        if self.repo_local_path.exists():
            msg = (
                f"Please delete the directory at {self.repo_local_path} and try again."
            )
            log.error(msg)
            raise FileExistsError(msg)

        _run("git", "clone", self.repo_uri, self.repo_local_path)
        _run("git", "checkout", self.repo_commit, cwd=self.repo_local_path)

        if self.system == "Windows":
            self._build_executable_windows()
        else:
            self._build_executable_unix()

        assert self.executable_path.exists()
        log.info(f"Executable created at {self.executable_path}")

    @abc.abstractmethod
    def _build_executable_unix(self):
        """Build the executable using 'make' on a UNIX-based system."""

    @abc.abstractmethod
    def _build_executable_windows(self):
        """Build the executable using 'MSBuild' on a Windows system."""


class DBGen(_TPCBuilder):
    """A class to help with creating and calling the TPC-H ``dbgen`` executable.

    Parameters
    ----------
    executable_path
        Path to the ``dbgen`` executable. If not given, ``datalogistik`` will attempt to
        make it by cloning a repo (requires ``git`` on your PATH) and building the tool
        (requires ``make`` for UNIX or ``msbuild`` for Windows on your PATH).
    """

    force_flag = "-f"
    scale_flag = "-s"
    partitions_flag = "-C"
    current_segment_flag = "-S"
    file_extension = "tbl"
    table_names = tpc_table_names["tpc-h"]
    repo_uri = "https://github.com/electrum/tpch-dbgen.git"
    repo_commit = "32f1c1b92d1664dba542e927d23d86ffa57aa253"
    repo_local_path = local_package_root / "dbgen_tool"
    repo_build_path = local_package_root / "dbgen_tool"

    def _get_default_executable_path(self):
        """Return the executable path for this generator if one isn't given."""
        if self.system == "Windows":
            return self.repo_build_path / "Debug" / "dbgen.exe"
        else:
            return self.repo_build_path / "dbgen"

    def _get_partitioned_filename(self, table_name, partition, total_partitions):
        if table_name == "nation":  # TODO: and total_partitions > some_number
            return f"nation.{self.file_extension}"
        if table_name == "region":
            return f"region.{self.file_extension}"
        return f"{table_name}.{self.file_extension}.{partition}"

    def _get_parallel_table_name_flags(self):
        tpch_table_abbrevs = ["c", "L", "n", "O", "P", "r", "s"]
        return [("-T", t) for t in tpch_table_abbrevs]

    def _get_serial_table_name_flags(self):
        # Generating table 'S' (partsupp) in parallel causes an error
        return [("-T", "S")]

    def _build_executable_unix(self):
        """Build the executable using 'make' on a UNIX-based system."""
        if self.system == "Darwin":
            _run("make", "MACHINE=MAC", cwd=self.repo_build_path)
        else:
            _run("make", "MACHINE=LINUX", cwd=self.repo_build_path)

    def _build_executable_windows(self):
        """Build the executable using 'MSBuild' on a Windows system."""
        log.info("Upgrading the solution file; this could take a few minutes...")
        solution_file = self.repo_build_path / "tpch.sln"
        devenv = _run("vswhere", "-property", "productPath")
        _run(devenv, solution_file, "/upgrade")

        log.info("Building the executable")
        _run("msbuild", solution_file, cwd=self.repo_build_path)

        # have to move this file for the executable to work
        distributions_file = self.repo_build_path / "dists.dss"
        distributions_file.replace(self.repo_build_path / "Debug" / "dists.dss")


class DSDGen(_TPCBuilder):
    """A class to help with creating and calling the TPC-DS ``dsdgen`` executable.

    Parameters
    ----------
    executable_path
        Path to the ``dsdgen`` executable. If not given, ``datalogistik`` will attempt
        to make it by cloning a repo (requires ``git`` on your PATH) and building the
        tool (requires ``make`` for UNIX or ``msbuild`` for Windows on your PATH).
    """

    force_flag = "-FORCE"
    scale_flag = "-SCALE"
    partitions_flag = "-PARALLEL"
    current_segment_flag = "-CHILD"
    file_extension = "dat"
    table_names = tpc_table_names["tpc-ds"]
    repo_uri = "https://github.com/gregrahn/tpcds-kit.git"
    repo_commit = "5a3a81796992b725c2a8b216767e142609966752"
    repo_local_path = local_package_root / "dsdgen_tool"
    repo_build_path = local_package_root / "dsdgen_tool" / "tools"

    def _get_default_executable_path(self):
        """Return the executable path for this generator if one isn't given."""
        if self.system == "Windows":
            return self.repo_build_path / "dsdgen.exe"
        else:
            return self.repo_build_path / "dsdgen"

    def _get_parallel_table_name_flags(self):
        return [
            ("-TABLE", t)
            for t in self.table_names
            if t not in ["web_returns", "store_returns", "catalog_returns"]
        ]

    def _get_serial_table_name_flags(self):
        return []

    def _get_partitioned_filename(self, table_name, partition, total_partitions):
        return f"{table_name}_{partition}_{total_partitions}.{self.file_extension}"

    def _build_executable_unix(self):
        """Build the executable using 'make' on a UNIX-based system."""
        if self.system == "Darwin":
            _run("make", "OS=MACOS", cwd=self.repo_build_path)
        else:
            _run("make", "OS=LINUX", cwd=self.repo_build_path)

    def _build_executable_windows(self):
        """Build the executable using 'MSBuild' on a Windows system."""
        log.info("Upgrading the solution file; this could take a few minutes...")
        solution_file = self.repo_build_path / "dbgen2.sln"

        # See https://github.com/gregrahn/tpcds-kit/blob/5a3a817/tools/dbgen2.sln
        # Some of these projects (dsqgen, checksum, and grammar) don't upgrade nicely.
        # We don't need them, so delete all references to them from this file.
        with open(solution_file, "r") as f:
            new_solution = "".join(
                line
                for line in f
                if not any(
                    uuid in line for uuid in ("59EBAD48", "3EA62CB9", "6540812A")
                )
            )
            new_solution = new_solution.replace("EndProject\nEndProject", "EndProject")
            new_solution = new_solution.replace("EndProject\nEndProject", "EndProject")
        with open(solution_file, "w") as f:
            f.write(new_solution)

        devenv = _run("vswhere", "-property", "productPath")
        _run(devenv, solution_file, "/upgrade")

        log.info("Building the executable")
        _run("msbuild", solution_file, cwd=self.repo_build_path)
