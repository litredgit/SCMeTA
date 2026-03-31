from .oxion import gen_scdata
from .format import *
from SCMeTA.tool import check_path

def load_data(
    path: str | dict,
    target_attr: str = "raw"
) -> dict[str, SCData]:
    """
    Load data from the specified path and return a dictionary of SCData objects.
    Args:
        path (str | dict): The path to the data file or a dictionary of file paths.
        target_attr (str): The attribute name to store the loaded data in the SCData object. Default is "raw".
    Returns:
        dict[str, SCData]: A dictionary where the keys are the file names and the values are the corresponding SCData objects containing the loaded data.
    """
    files = check_path(path, do='r', type=["raw", "mzml"])
    return {name: gen_scdata(name, path, target_attr) for name, path in files.items()}



