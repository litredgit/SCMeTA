import pandas as pd
import numpy as np

def find_cell(
        mat: pd.DataFrame,
        refer_mz: float = 760.58,
        max_ratio: float = 0.1
) -> list[list]:
    
    if refer_mz not in mat.columns:
        raise ValueError(f"refer_mz {refer_mz} not found in mat columns.")
    xic = mat[refer_mz]  # extract NumPy Ndarray

    # find cell index
    max_intensity = np.max(xic)
    cell_region = xic.index[xic > max_intensity * max_ratio].to_numpy()
    if len(cell_region) == 0:
        raise ValueError("No cell found.")

    # find adjacent
    diffs = np.diff(cell_region)
    breaks = np.where(diffs > 1)[0] + 1
    regions = np.split(cell_region, breaks)

    return [[r[0], r[-1]] for r in regions]


def merge_cell(
    mat: pd.DataFrame, cell_pos: list[list], adjacent: int = 3
) -> pd.DataFrame:
    """Combine the cell regions.

    Parameters
    ----------
    mat : pd.DataFrame
        The raw data.
    cell_pos : list[list]
        The cell regions.
    adjacent : int, optional
        The adjacent cell regions, by default 3

    Returns
    -------
    pd.DataFrame
        The combined cell regions.
    """
    cell_series_list = []
    index_list = []
    for index, group in enumerate(cell_pos):
        if len(group) <= adjacent:
            cell_series_list.append(mat.loc[group].sum())
            index_list.append(index)
    cell_mat = pd.concat(cell_series_list, axis=1).T
    # Set index
    cell_mat.index = index_list
    cell_mat.index.name = "Scan"
    return cell_mat
