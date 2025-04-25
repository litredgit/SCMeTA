import pandas as pd
import numpy as np



def find_cell_index(xic: pd.DataFrame, max_ratio=0.1) -> list:
    max_intensity = xic.max()
    cell_region = xic[xic > max_ratio * max_intensity].index
    return cell_region.to_list()


def find_adjacent(cell_array: list) -> list[list]:
    cell_group = []
    for i in cell_array:
        if not cell_group or i != cell_group[-1][-1] + 1:
            cell_group.append([i])
        else:
            cell_group[-1].append(i)
    return cell_group


def find_cell(
    mat: pd.DataFrame, refer_mz: float = 760.58, max_ratio: float = 0.1
) -> list[list]:
    """Find the cell regions.

    Parameters
    ----------
    mat : pd.DataFrame
        The raw data.
    refer_mz : float, optional
        The reference m/z, by default 760.58
    max_ratio : float, optional

    Returns
    -------
    list[list]
        The cell regions.
    """
    xic = mat.loc[:, refer_mz].fillna(0)
    cell_array = find_cell_index(xic, max_ratio=max_ratio)
    return find_adjacent(cell_array)

def find_cell_fast(
        mat: pd.DataFrame,
        refer_mz: float = 760.58,
        max_ratio: float = 0.1
) -> list[list]:
    """Optimized cell detection using NumPy."""
    if refer_mz not in mat.columns:
        print(f"Warning: refer_mz {refer_mz} not found in mat columns.")
        return []  # 或者返回其他默认值
    xic = mat[refer_mz].values  # 提取 NumPy 数组
    xic = np.nan_to_num(xic, nan=0.0)  # 替换 NaN

    # 找满足条件的索引
    max_val = np.max(xic)
    threshold = max_val * max_ratio
    cell_indices = np.where(xic >= threshold)[0]

    # 合并相邻区域
    if len(cell_indices) == 0:
        return []

    diffs = np.diff(cell_indices)
    breaks = np.where(diffs > 1)[0] + 1
    regions = np.split(cell_indices, breaks)

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
