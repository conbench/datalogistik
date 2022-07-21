import abc
import os
import pathlib
import platform
import shutil
import subprocess
from typing import List, Optional

from .tpc_info import tpc_table_names


DBGEN_REPO_URI = "https://github.com/electrum/tpch-dbgen"
DBGEN_REPO_COMMIT = "32f1c1b92d1664dba542e927d23d86ffa57aa253"
DBGEN_REPO_MAKEFILE_NAME = "makefile"
DBGEN_REPO_MAKEFILE_MACHINE_LINE = "MACHINE = MAC"

local_dbgen_repo = pathlib.Path(__file__).parent.resolve() / "dbgen_tool"


def _run(*args, return_output=False, **kwargs) -> str:
    """Run a command in the shell, checking its return code and maybe returning output.

    Parameters
    ----------
    *args
        Command to execute
    return_output
        Whether to return stdout/stderr as a string. Default False so the command prints
        as it runs instead of after it completes.
    **kwargs
        Keyword args to subprocess.run()
    """
    if return_output:
        res = subprocess.run(
            args,
            **kwargs,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        output = res.stdout.strip()
        print(output)
        return output
    else:
        subprocess.run(args, **kwargs, check=True)


class _TPCBuilder(abc.ABC):
    """A class to help with creating and calling a TPC database generator.

    Parameters
    ----------
    executable_path
        Path to the generator executable for this TPC database.
    """

    executable_path: Optional[pathlib.Path] = None

    force_flag: str
    scale_flag: str
    file_extension: str
    table_names: List[str]

    def __init__(self, executable_path: Optional[pathlib.Path]):
        if executable_path:
            executable_path = pathlib.Path(executable_path).resolve()
            if not executable_path.exists():
                raise ValueError(
                    f"The given executable_path '{executable_path}' does not exist."
                )
            self.executable_path = executable_path

    def create_dataset(self, out_dir: pathlib.Path, scale_factor: int = 1):
        """Call the executable to generate the TPC database.

        Parameters
        ----------
        out_dir
            The directory to place the CSVs in.
        scale_factor
            The scale factor to use when building the database.
        """
        if not self.executable_path or not self.executable_path.exists():
            print("Could not find an executable. Attempting to create one.")
            self._make_executable()

        _run(
            self.executable_path,
            self.force_flag,
            self.scale_flag,
            str(scale_factor),
            cwd=self.executable_path.parent,
        )

        # Move the new files to out_dir
        out_dir = pathlib.Path(out_dir).resolve()
        for table_name in self.table_names:
            old_file = self.executable_path.parent / (table_name + self.file_extension)
            new_file = out_dir / (table_name + ".csv")
            shutil.move(old_file, new_file)
            # reset permissions to read-only
            os.chmod(new_file, 0o444)
            print("Created", new_file)

    @abc.abstractmethod
    def _make_executable(self):
        """Clone the repo and build the executable."""


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
    file_extension = ".tbl"
    table_names = tpc_table_names["tpc-h"]

    def __init__(self, executable_path: Optional[pathlib.Path] = None):
        super().__init__(executable_path=executable_path)
        self.system = platform.system()

        # If not given, clone the repo into datalogistik's module directory
        if not self.executable_path:
            if self.system == "Windows":
                self.executable_path = local_dbgen_repo / "Debug" / "dbgen.exe"
            else:
                self.executable_path = local_dbgen_repo / "dbgen"

    def _make_executable(self):
        """Clone the repo and build the executable."""
        if local_dbgen_repo.exists():
            raise FileExistsError(
                f"Please delete the directory at {local_dbgen_repo} and try again."
            )

        _run("git", "clone", DBGEN_REPO_URI, local_dbgen_repo)
        _run("git", "checkout", DBGEN_REPO_COMMIT, cwd=local_dbgen_repo)

        if self.system == "Linux":
            self._build_executable_unix(machine="LINUX")
        elif self.system == "Darwin":
            self._build_executable_unix(machine="MAC")
        elif self.system == "Windows":
            self._build_executable_windows()
        else:
            raise NotImplementedError(f"System '{self.system}' is not supported yet.")

        assert self.executable_path.exists()
        print(f"Executable created at {self.executable_path}")

    def _build_executable_unix(self, machine: str):
        """Modify the Makefile, and build the executable using 'make' on a UNIX-based
        system.

        Parameters
        ----------
        machine
            The 'MACHINE' choice for the Makefile.
        """
        makefile_path = local_dbgen_repo / DBGEN_REPO_MAKEFILE_NAME
        with open(makefile_path, "r") as f:
            makefile_contents = f.read()
        with open(makefile_path, "w") as f:
            f.write(
                makefile_contents.replace(
                    DBGEN_REPO_MAKEFILE_MACHINE_LINE, f"MACHINE = {machine}"
                )
            )

        _run("make", cwd=local_dbgen_repo)

    def _build_executable_windows(self):
        """Build the executable using 'MSBuild' on a Windows system."""
        print("Upgrading the solution file; this could take a few minutes...")
        solution_file = local_dbgen_repo / "tpch.sln"
        devenv = _run("vswhere", "-property", "productPath", return_output=True)
        _run(devenv, solution_file, "/upgrade")

        print("Building the executable")
        _run("msbuild", solution_file, cwd=local_dbgen_repo)

        # have to move this file for the executable to work
        distributions_file = local_dbgen_repo / "dists.dss"
        distributions_file.replace(local_dbgen_repo / "Debug" / "dists.dss")


class DSDGen(_TPCBuilder):
    """A class to help with creating and calling the TPC-DS ``dsdgen`` executable.

    Parameters
    ----------
    executable_path
        Path to the ``dsdgen`` executable. Required for now.
    """

    force_flag = "-FORCE"
    scale_flag = "-SCALE"
    file_extension = ".dat"
    table_names = tpc_table_names["tpc-ds"]

    def _make_executable(self):
        """Clone the repo and build the executable."""
        raise NotImplementedError("datalogistik cannot build dsdgen for you yet.")
