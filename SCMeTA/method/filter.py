import numpy as np
import pandas as pd

from SCMeTA.config import INCLUDE_LIST, EXCLUDE_LIST
from collections import defaultdict

def sum_df(df: pd.DataFrame, scan: int) -> pd.DataFrame:
    df = df.groupby("Mass").sum().reset_index()
    df.insert(0, "Scan", scan)
    df = df.set_index("Scan")
    return df

def peaks_combine(_raw: pd.DataFrame, resolution: float = 0.01) -> pd.DataFrame:
    # round mass to given resolution
    _raw["Mass"] = np.floor(_raw["Mass"] / resolution) * resolution

    # combine peaks with the same mass
    scans = _raw.index.unique()
    temp = [sum_df(_raw.loc[[scan]], scan) for scan in scans]  # 注意双括号
    return pd.concat(temp)

def filter_occ(
    raw: pd.DataFrame, resolution: float = 0.01, count: int = 10
) -> pd.DataFrame:
    """
    Filter out peaks that occur less than count times in the data.
    :param raw: Raw data.
    :param resolution: Resolution of m/z.
    :param count: Minimum number of occurrences.
    :return: List of filtered peaks.
    """
    # do peaks combine
    process = peaks_combine(raw, resolution)
    # extract unique peak list
    peaks = process["Mass"].value_counts()
    # filter peaks occur less than count times
    peaks = peaks[peaks >= count]
    process = process[process["Mass"].isin(peaks.index)]
    return process

# def filter_mat(
#     mat_list: list[pd.DataFrame], threshold: float = 0.2, lock: bool = False, method: str = "all"
# ) -> list[pd.DataFrame]:
#     """Filter matrix by count
#     Args:
#         mat_list (list[pd.DataFrame]): List of matrix
#         threshold (float, optional): Threshold. Defaults to 0.2.
#         lock (bool, optional): Whether to lock the mz list. Defaults to False.
#     Returns:
#         pd.DataFrame: Filtered matrix
#     """
#     if method == "all":
#         count = sum([mat.shape[0] for mat in mat_list]) * threshold
#         large_mat = pd.concat(mat_list)
#         large_mat.sort_index(axis=1, inplace=True)
#         mz_bool = large_mat.count() >= count
#         if lock:
#             include_bool = large_mat.columns.isin(INCLUDE_LIST)
#             exclude_bool = large_mat.columns.isin(EXCLUDE_LIST)
#             mz_bool = mz_bool | include_bool
#             mz_bool = mz_bool & ~exclude_bool
#         mz_list = mz_bool[mz_bool].index
#         # if mat in mat_list doesn't have the mz, it will be filled with NaN
#         mat_list = [mat.reindex(columns=mz_list, fill_value=None) for mat in mat_list]
#
#         mat_list = [mat[mz_list] for mat in mat_list]
#         return mat_list
#     elif method == "any":
#         index_lists = []
#         for mat in mat_list:
#             count = mat.shape[0] * threshold
#             mz_bool = mat.count() >= count
#             if lock:
#                 include_bool = mz_bool.index.isin(INCLUDE_LIST)
#                 exclude_bool = mz_bool.index.isin(EXCLUDE_LIST)
#                 mz_bool = mz_bool | include_bool
#                 mz_bool = mz_bool & ~exclude_bool
#             index_lists.append(mz_bool[mz_bool])
#         mz_bool = pd.concat(index_lists, axis=1)
#         mz_list = mz_bool.index
#         new_mat_list = []
#         for mat in mat_list:
#             mat = mat.reindex(columns=mz_list, fill_value=None)
#             mat = mat[mz_list]
#             new_mat_list.append(mat)
#         return new_mat_list可以work

def filter_mat(mat_list, threshold=0.2, lock=False, method="all"):
    """基于生成器的矩阵过滤"""
    # 第一步：流式计算mz_list
    mz_counts = defaultdict(int)
    for mat in mat_list:
        for mz in mat.columns:
            mz_counts[mz] += (mat[mz] > 0).sum()

    threshold_count = sum(m.shape[0] for m in mat_list) * threshold
    mz_list = [mz for mz, cnt in mz_counts.items()
               if cnt >= threshold_count or (lock and mz in INCLUDE_LIST)]

    # 第二步：惰性过滤
    for mat in mat_list:
        yield mat.reindex(columns=mz_list, fill_value=0)


if __name__ == '__main__':
    # func any threshold
    # func all threshold
    pass