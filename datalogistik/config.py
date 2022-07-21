import pathlib

debug = True

default_repo_file = str(pathlib.Path(__file__).parent.parent / "repo.json")
default_cache_location = "./"
cache_dir_name = "datalogistik_cache"
metadata_filename = "datalogistik_metadata.ini"
supported_formats = ["parquet", "csv"]
hashing_chunk_size = 16384
