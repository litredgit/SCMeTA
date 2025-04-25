import numpy as np
import pandas as pd

from SCMeTA.config import INCLUDE_LIST, EXCLUDE_LIST
from collections import defaultdict

# def sum_df(df, scan) -> pd.DataFrame:
#     df = df.groupby("Mass").sum().reset_index()
#     df.insert(0, "Scan", scan)
#     df = df.set_index("Scan")
#     return df

# def sum_df(df, scan) -> pd.DataFrame:
#     """处理单扫描数据（兼容Series和DataFrame输入）"""
#     # 转换为DataFrame（如果是Series）
#     if isinstance(df, pd.Series):
#         df = df.to_frame().T可以work  # 转置使成为单行DataFrame
def sum_df(df_scan: pd.DataFrame, scan: int) -> pd.DataFrame:
    # 假设 sum_df 是对每个 scan 的 DataFrame 进行求和或其他聚合操作
    # 优化：使用 agg() 或 groupby().sum() 替代逐行操作
    summed = df_scan.groupby("Mass").sum().reset_index()  # 按 Mass 合并相同值
    summed["scan"] = scan  # 添加 scan 列
    return summed

    # 列名验证
    required_cols = {"Mass", "Intensity"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"Missing columns: {missing}. Actual columns: {list(df.columns)}")

    # 分组求和
    df = df.groupby("Mass", as_index=False).agg({"Intensity": "sum"})
    df.insert(0, "Scan", int(scan))
    return df.set_index("Scan")


# def peaks_combine(_raw: pd.DataFrame, resolution: float = 0.01) -> pd.DataFrame:
#     try:
#         _raw["Mass"] = _raw["Mass"].divide(resolution).apply(np.floor).mul(resolution)
#     except AttributeError:
#         _raw["Mass"] = np.floor(_raw["Mass"] / resolution) * resolution
#
#     # 修复点：使用 DataFrame.loc[[scan]] 而非 .loc[scan] 确保返回 DataFrame
#     scans = _raw.index.unique()
#     temp = [sum_df(_raw.loc[[scan]], scan) for scan in scans]  # 注意双括号
#     return pd.concat(temp)

# def peaks_combine(_raw: pd.DataFrame, resolution: float) -> pd.DataFrame:
#     """兼容 Scan 列模式"""
#     _raw = _raw.copy()
#     _raw["Mass"] = (_raw["Mass"] / resolution).astype(int) * resolution
#
#     # 按 Scan 和 Mass 分组求和（Scan 是列）
#     grouped = _raw.groupby(["Scan", "Mass"], as_index=False).agg({"Intensity": "sum"})
#     return grouped.set_index("Scan")可以work  # 最终结果设 Scan 为索引

def peaks_combine(_raw: pd.DataFrame, resolution: float = 0.01) -> pd.DataFrame:
    # 1. 四舍五入 Mass 到指定分辨率
    _raw["Mass"] = (_raw["Mass"] / resolution).apply(np.floor) * resolution

    # 2. 使用 groupby + apply 替代 for 循环（快 10-100 倍）
    grouped = _raw.groupby(_raw.index)  # 按 scan 分组
    temp = grouped.apply(lambda x: sum_df(x, x.name))  # x.name 是 scan 编号
    return temp.reset_index(drop=True)  # 返回合并后的 DataFrame

# def filter_occ(
#     raw: pd.DataFrame, resolution: float = 0.01, count: int = 10
# ) -> pd.DataFrame:
#     """
#     Filter out peaks that occur less than count times in the data.
#     :param raw: Raw data.
#     :param resolution: Resolution of m/z.
#     :param count: Minimum number of occurrences.
#     :return: List of filtered peaks.
#     """
#     process = peaks_combine(raw, resolution)
#     peaks = process["Mass"].value_counts()
#     peaks = peaks[peaks >= count]
#     process = process[process["Mass"].isin(peaks.index)]
#     return process可以work
def filter_occ(df: pd.DataFrame, resolution: float = 0.01, count: int = 10) -> pd.DataFrame:
    # 优化：使用 groupby + size 计算每个 Mass 的出现次数
    mass_counts = df.groupby("Mass").size().reset_index(name="count")
    # 使用布尔索引过滤（比逐行循环快 100 倍）
    filtered_masses = mass_counts[mass_counts["count"] >= count]["Mass"]
    # 返回过滤后的 DataFrame
    return df[df["Mass"].isin(filtered_masses)]

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