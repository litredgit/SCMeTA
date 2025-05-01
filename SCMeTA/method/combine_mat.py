import pandas as pd


def combine_mat(mat_list) -> pd.DataFrame:
    """
    Combine a list of matrices into a single matrix.
    """
    combined = pd.concat(mat_list)
    # convert the column_index to string
    combined.columns = combined.columns.astype(str)
    return combined.sort_index(axis=1)
