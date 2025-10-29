import os

from SCMeTA.accelerate import MultiProcessing
from SCMeTA.accelerate import MultiThreader

try:
    from .thermo import load_thermo_data
except ImportError as e:
    print(f"Warning: Thermo RawFileReader import failed: {e}")
from .process import load_process_data
from .mzML import load_mzML_data
from .format import *
from .database import load_from_database
from SCMeTA.tool import check_path

FUNC_DICT = {
    "thermo": load_thermo_data,
    "process": load_process_data,
    "mzML":load_mzML_data
}

SUFFIX_DICT = {
    "thermo": [".raw", ".txt"],
    "process": [".csv"],
    "mzML": [".mzML", ".mzml"]
}

def path_from_database(path: str) -> str:
    base_path = ".Database/cyesi"
    return os.path.join(base_path, path)


def read_files_in_parallel(names: list[str], paths: list[str], data_type: str = "thermo", target_attr: str = "raw", load_range: tuple[int, int] | None = None, multi_method: str = "MultiThread") -> dict[str, SCData]:
    if multi_method == "MultiProcess":
        args = dict(zip(names, paths))
        args["target_attr"] = target_attr
        args["load_range"] = load_range
        mp = MultiProcessing()
        results = mp.run(func=FUNC_DICT[data_type], data=args)
    elif multi_method == "MultiThread":
        args = [(name, path, target_attr, load_range) for name, path in zip(names, paths)]
        mt = MultiThreader()
        results = mt.run(FUNC_DICT[data_type], args)
    else:
        raise ValueError("Method not found, choose from (\"mp\" or \"mt\")")
    return results


def load_data(
    path: str | dict, data_type: str = "thermo", target_attr: str = "raw", load_range: tuple[int, int] | None = None, method: str = "MultiThread"
) -> dict[str, SCData]:
    """
    Load data from the given path or database.
    """
    files = check_path(path, do='r', type=[ext.lstrip('.') for ext in SUFFIX_DICT[data_type]])
    
    if method == "one by one":
        return {name: FUNC_DICT[data_type](name, file_path, target_attr, load_range) for name, file_path in files.items()}
    else:
        return read_files_in_parallel(list(files.keys()), list(files.values()), data_type, target_attr, load_range, method)

    # elif isinstance(path, dict):
    #     paths = [path_from_database(path) for path in path.values()]
    #     results = read_files_in_parallel(path.keys(), paths, data_type, target_attr, load_range, method)
    #     return results


