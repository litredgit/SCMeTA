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
    # _raw["Mass"] = np.around(_raw["Mass"], decimals=np.log10(1 / resolution))
    # to be consistent with the original code, we use floor, as 'around' does not solve the issue of combining peaks
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

def filter_mat(mat_list: list[pd.DataFrame], threshold: float = 0.2, lock: bool = False, method: str = "all"):
    mz_counts = defaultdict(int)
    if method == "all":
        # Filter all matrices together
        # Combine all matrices and count occurrences
        for mat in mat_list:
            for mz in mat.columns:
                mz_counts[mz] = (mat[mz] > 0).sum()
        # Filter based on threshold
        threshold_count = sum(mat.shape[0] for mat in mat_list) * threshold
        mz_list = [mz for mz, cnt in mz_counts.items() 
                   if  (not lock and cnt >= threshold_count) or (lock and mz in INCLUDE_LIST and mz not in EXCLUDE_LIST)]
        # Reindex matrices to keep only the filtered m/z values
        for mat in mat_list:
            yield mat.reindex(columns=mz_list, fill_value=0)
    elif method == "any":
        # Filter each matrix separately
        all_mz = set()
        for mat in mat_list:
            for mz in mat.columns:
                mz_counts[mz] = (mat[mz] > 0).sum()
            threshold_count = mat.shape[0] * threshold
            mz_list = [mz for mz, cnt in mz_counts.items() 
                       if  (not lock and cnt >= threshold_count) or (lock and mz in INCLUDE_LIST and mz not in EXCLUDE_LIST)]
            mat.reindex(columns=mz_list, fill_value=0)
            all_mz.update(mz_list)
        all_mz = sorted(all_mz)
        for mat in mat_list:
            yield mat.reindex(columns=all_mz)
    elif method == "none":
        for mat in mat_list:
            yield mat 
    else:
        raise ValueError(f"Unknown method: {method}. Use 'all' or 'any'.")

if __name__ == '__main__':
    # func any threshold
    # func all threshold
    pass