import os

from SCMeTA.accelerate import MultiProcessing
from SCMeTA.accelerate import MultiThreader

from .thermo import load_thermo_data
from .process import load_process_data
from .waters import load_waters_data
from .mzML import load_mzML_data
from .format import *
from .database import load_from_database

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # True when type checking, False when running
    from .mzML import load_mzml, load_data
else:
    # do when running
    def __getattr__(name):
        if name in ("load_mzml", "load_data"):
            from .mzML import load_mzml, load_data
            return locals()[name]
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

FUNC_DICT = {
    "thermo": load_thermo_data,
    "process": load_process_data,
    "waters": load_waters_data,
    "mzML":load_mzML_data
}

SUFFIX_DICT = {
    "thermo": [".raw", ".txt"],
    "process": [".csv"],
    "waters": [".wiff", ".txt"],
    "mzML": [".mzML", ".mzml"]
}


def is_type(path: str, data_type: str) -> bool:
    suffix = os.path.splitext(path)[1]
    return suffix.lower() in SUFFIX_DICT[data_type]


def get_name_from_path(path: str) -> str:
    return os.path.splitext(os.path.basename(path))[0]


def path_from_database(path: str) -> str:
    base_path = ".Database/cyesi"
    return os.path.join(base_path, path)


def read_files_in_parallel(paths: list[str], names = None, data_type: str = "thermo", multi_method: str = "MultiThread") -> dict[str, SCData]:
    if names is None:
        names = [get_name_from_path(path) for path in paths]
    if multi_method == "MultiProcess":
        args = dict(zip(names, paths))
        mp = MultiProcessing()
        results = mp.run(func=FUNC_DICT[data_type], data=args)
    elif multi_method == "MultiThread":
        args = [(name, path) for name, path in zip(names, paths)]
        mt = MultiThreader()
        results = mt.run(FUNC_DICT[data_type], args)
    else:
        raise ValueError("Method not found, choose from (\"mp\" or \"mt\")")
    return results


def load_data(
    path: str | dict, name: str | None = None, data_type: str = "thermo", method: str = "MultiThread"
) -> dict[str, SCData]:
    if isinstance(path, str):
        if os.path.isdir(path):
            files = [file for file in os.listdir(path) if is_type(file, data_type)]
            paths = [os.path.join(path, file) for file in files]
            if method == "one by one":
                return {name:FUNC_DICT[data_type](name, path) for name, path in zip(files, paths) if is_type(name, data_type)} 
            else:
                return read_files_in_parallel(paths=paths, data_type=data_type, multi_method=method)
        elif os.path.isfile(path):
            if not is_type(path, data_type):
                raise ValueError(f"File {path} is not a {data_type} file")
            if name is None:
                name = get_name_from_path(path)
            return {name: FUNC_DICT[data_type](name, path)}
        else:
            raise ValueError("Please provide a valid path")
    elif isinstance(path, dict):
        paths = [path_from_database(path) for path in path.values()]
        results = read_files_in_parallel(paths=paths, names=path.keys(), data_type=data_type)
        return results
    else:
        raise ValueError(f"path{path} not str or dict")


