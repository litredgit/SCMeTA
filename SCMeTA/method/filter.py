import pandas as pd
from collections import defaultdict

def filter_occ(
    raw: pd.DataFrame, count: int = 10
) -> pd.DataFrame:
    """
    Filter out peaks that occur less than count times in the data.
    :param raw: Raw data.
    :param resolution: Resolution of m/z.
    :param count: Minimum number of occurrences.
    :return: List of filtered peaks.
    """
    # extract unique peak list
    peaks = raw["Mass"].value_counts()
    # filter peaks occur less than count times
    peaks = peaks[peaks >= count]
    process = raw[raw["Mass"].isin(peaks.index)]
    return process

def filter_mat(mat_list: list[pd.DataFrame], threshold: float = 0.2, lock: bool = False, method: str = "all",
               INCLUDE_LIST: list[float] = None, EXCLUDE_LIST: list[float] = None):
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