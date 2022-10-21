================
``datalogistik``
================

This tool can download, generate, convert, and cache datasets. It uses a dataset
metadata file that contains URLs to download known existing datasets. It is able to
generate some datasets such as TPC-H by calling their (external) generator program (e.g.
``dbgen``).

Usage::

    datalogistik <generate|cache>

    datalogistik generate [-h] \
        -d DATASET \
        -f FORMAT \
        [-s SCALE_FACTOR] \
        [-g GENERATOR_PATH] \
        [-c COMPRESSION]

    datalogistik cache [-h] \
        [--clean] \
        [--prune-entry ENTRY] \
        [--prune-invalid] \
        [--validate]


``DATASET``
    Name of the dataset as specified in the repository, or one of the supported
    generators (``tpc-h``, ``tpc-ds``).

``FORMAT``
    File format to instantiate the dataset in. If the original dataset specified in the
    repository has a different format, it will be converted. Supported formats:
    ``parquet``, ``csv``, ``arrow``.

``SCALE_FACTOR``
    Scale factor for generating TPC data. Default 1.

``COMPRESSION``
    Compression to be used for the dataset. For Parquet dataset, this value will be
    passed to the parquet writer.
    For CSV datasets, supported values are gz (for GZip) or none.

``clean``
    Perform a clean-up of the cache, checking whether all of the subdirectories 
    are part of a dataset that contains a valid metadata file. 
    Otherwise, they will be removed.
    This option is helpful after manually removing directories from the cache.

``prune-entry``
    Remove a given subdirectory from the cache. The user can specify a certain
    particular dataset (e.g. ``tpc-h/1/parquet/0``), or a directory higher in the hierarchy
    (e.g. ``tpc-h/100``).

``prune-invalid``
    Validate all entries in the cache for file integrity and remove entries that fail.

``validate``
    Validate all entries in the cache for file integrity and report entries that fail.

Installing using ``pipx`` (recommended)
---------------------------------------

`pipx <https://pypa.github.io/pipx/>`_ is a CLI tool installer that keeps each tool's
dependencies isolated from your working python session and from other tools. This means
you won't have to deal with any dependency version conflicts with ``datalogistik``, and
if you change one of ``datalogistik``'s dependencies (like ``pyarrow``), the tool will
still work.

Install ``pipx``::

    pip install pipx
    pipx ensurepath

**Note: after this, you need to restart your terminal session!**

Install ``datalogistik``::

    pipx install git+https://github.com/conbench/datalogistik.git

Run ``datalogistik``::

    datalogistik -d type_floats -f csv

Installing using ``pip``
------------------------

If you are okay with dealing with potential dependency problems, you may install the
package with ``pip``::

    pip install git+https://github.com/conbench/datalogistik.git

Run ``datalogistik``::

    datalogistik -d type_floats -f csv

Installing from source
----------------------

For local development of the package, you may install from source.

First install `poetry <https://python-poetry.org/docs/master/#installing-with-pipx>`_.
There are a few ways to do so (see the link), but ``pipx`` seems like the easiest and
safest.

Clone the repo::

    git clone https://github.com/conbench/datalogistik.git
    cd datalogistik

Install ``datalogistik`` dependencies::

    poetry install
    source $(poetry env info --path)/bin/activate
    pre-commit install

Run the checks that will be run in CI::

    # Lint the repo
    pre-commit run --all-files
    # Run unit tests
    pytest
    # Run integration test
    datalogistik -d tpc-h -f parquet

TPC Generators
--------------
The location of dbgen (the generator for TPC-H data) and dsdgen (the generator for TPC-DS data)
can be specified by setting the environment variable ``DATALOGISTIK_GEN``.
If it is not set, ``datalogistik`` will clone them from a publicly available repo on Github
and build from source.

Caching
-------

By default, ``datalogistik`` caches datasets to the local directory
``./datalogistik_cache``. This directory is created if it does not exist yet. The
location is the current working directory, but that can be overridden by setting the
``DATALOGISTIK_CACHE`` environment variable. It stores each instance of a dataset that
the user has requested to instantiate, in addition to different file formats. There is no manifest that lists what entries are in the cache.
``datalogistik`` searches the cache by using its directory structure:

TPC datasets
    ``datalogistik_cache/<name>/<scale-factor>/<format>/``

Other datasets
    ``datalogistik_cache/<name>/<format>/``

Each entry in the cache has a metadata file called `datalogistik_metadata.ini`_.

Conversion
----------

``datalogistik`` uses ``pyarrow`` to convert between formats. It is able to convert
datasets that are too large to fit in memory by using the ``pyarrow`` Datasets API.


Repositories
------------

``datalogistik`` uses a metadata repository file for finding downloadable datasets. By
default, it searches for a file ``./repo.json`` in the working directory, but you can
override this by setting the ``DATALOGISTIK_REPO`` environment variable. You can also
point it to a JSON file accessible online via http.

The default ``repo.json`` file included is based on sources taken from `the arrowbench
repo <https://github.com/ursacomputing/arrowbench/blob/main/R/known-sources.R>`_.

A repository JSON file contains a list of entries, where each entry has the following
properties:

``name``
    A string to identify the dataset.

``url``
    Location where this dataset can be downloaded (for now, http(s). Support for S3 and
    GCS may follow later).

``format``
    File format (e.g. csv, parquet).


In addition, entries can have the following optional properties:

``delim``
    The character used as field delimiter (e.g. ",").

``dim``
    Dimensions ([rows, columns]).

``compression``
    File-level compression (e.g. gz for GZip), that needs to be decoded before an
    application can use the file. Some formats like parquet use internal compression,
    but that is not what is meant here.

``schema``
    The schema of the tabular data in the file.
    The structure of a schema is a JSON string with key:value pairs for each column.
    The key is the column name, and the value is either the name of an Arrow datatype
    without any parameters, or a dictionary with the following properties:
    - type_name: Name of an Arrow datatype
    - arguments: either a dictionary of argument_name:value items, a list of values,
    or a single value.
    Example:
.. code::

    {
        "a": "string",
        "b": {"type_name": "timestamp", "arguments": {"unit": "ms"}},
        "c": {"type_name": "decimal", "arguments": [7, 3]}
    }

``header_line``
    Boolean denoting whether the first line of a CSV file contains the column names (default: false)

Output
--------------

Upon success, a JSON string is output on stdout. It points to the dataset created in the cache.
It contains the following properties:

``name``
    String to identify the dataset.

``format``
    File format (e.g. csv, parquet) - note that this may differ from the information in
    the repo, because ``datalogistik`` might have performed a format conversion.

``scale_factor``
    (optional) In case of a TPC dataset, the scale factor.

``delim``
    The character used as field delimiter (e.g. ",").

``dim``
    Dimensions ([rows, columns]).


The dataset itself contains a metadata file with the following additional properties:

datalogistik_metadata.ini
~~~~~~~~~~~~~~~~~~~~~~~~~

``local_creation_date``
    Date and time when this dataset was downloaded or generated to the cache.


``url``
    The location where this dataset was downloaded.

``homepage``
    Location where more information about the origins of dataset can be found.

``tables``
    A list of tables in the dataset, each with its own (set of) files. Each entry in the
    list has the following properties:

    ``table``
        Name of the table.

    ``schema``
        Schema of the table.

    ``url``
        Download url in case this is a single-file table.

    ``base_url``
        Base download url in case this is a multi-file table. Each file will append
        their `rel_path` to this to form the full download url.

    ``files``
        A list of files in this table. Each entry in the list has the following properties:

        ``rel_path``
            Path to the file, relative to the directory of this table.

        ``file_size``
            Size of the file.

        ``md5``
            MD5 checksum of the file.

License info
------------
Copyright (c) 2022, Voltron Data.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
