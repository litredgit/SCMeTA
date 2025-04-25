import pandas as pd


def combine_mat(mat_list) -> pd.DataFrame:
    """
    Combine a list of matrices into a single matrix.
    """
    # return pd.concat(mat_list).sort_index(axis=1)
    combined = pd.concat(mat_list)
    # 将列索引转换为字符串类型
    combined.columns = combined.columns.astype(str)
    return combined.sort_index(axis=1)
