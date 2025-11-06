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

def path_from_database(path: str) -> str:
    base_path = ".Database/cyesi"
    return os.path.join(base_path, path)

def read_files_in_parallel(
        files: dict[str, str],
        reader: callable,
        target_attr: str = "raw",
        load_range: tuple[int, int] | None = None,
        multi_method: str = "mt"
) -> dict[str, SCData]:
    """
    Read files in parallel using the specified method.
    """
    if multi_method == "mp":
        args = files.copy()
        args["target_attr"] = target_attr
        args["load_range"] = load_range
        mp = MultiProcessing()
        results = mp.run(func=reader, data=args)
    elif multi_method == "mt":
        args = [(name, path, target_attr, load_range) for name, path in files.items()]
        mt = MultiThreader()
        results = mt.run(reader, args)
    else:
        raise ValueError("Method not found, choose from (\"mp\" or \"mt\")")
    return results

def load_data(
    path: str | dict,
    data_type: str = "auto",
    target_attr: str = "raw", 
    load_range: tuple[int, int] | None = None, 
    method: str = "mt"
) -> dict[str, SCData]:
    """
    Load data from the given path or database. only support one type of file in parallel mode.
    Parameters:
        path (str | dict): The file path or a dictionary of file paths.
        target_attr (str): The target attribute to load data into.
        load_range (tuple[int, int] | None): The range of data to load.
        method (str): The method to use for loading data ("seq", "mt", "mp").
    Returns:
        dict[str, SCData]: A dictionary of loaded SCData objects.
    """
    FUNC_DICT = {
        "thermo": load_thermo_data,
        "process": load_process_data,
        "mzml":load_mzML_data
    }

    SUFFIX_DICT = {
        "thermo": ["raw", "txt"],
        "process": ["csv"],
        "mzml": ["mzml"]
    }

    files, readers = {}, {}
    if data_type == "auto":
        for dtype in SUFFIX_DICT.keys():
            try:
                files_dtype=check_path(path, do='r', type=[ext for ext in SUFFIX_DICT[dtype]])
                files.update(files_dtype)
                readers.update({name: FUNC_DICT[dtype] for name in files_dtype.keys()})
            except Exception:
                pass
    else:
        files = check_path(path, do='r', type=[ext for ext in SUFFIX_DICT[data_type]])
        readers.update({name: FUNC_DICT[data_type] for name in files.keys()})

    if method == "seq":
        return {name: readers[name](name, path, target_attr, load_range) for name, path in files.items()}
    else:
        # only support one type of reader in parallel mode
        reader = next(iter(readers.values()))
        return read_files_in_parallel(files, reader, target_attr, load_range, method)

    # elif isinstance(path, dict):
    #     paths = [path_from_database(path) for path in path.values()]
    #     results = read_files_in_parallel(path.keys(), paths, target_attr, load_range, method)
    #     return results


