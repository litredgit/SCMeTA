import pandas as pd

def combine_peaks(mat: pd.DataFrame, mz_interval: float = 0.01) -> pd.DataFrame:
    # Combine the columns which the difference is less than 0.01
    columns = mat.columns
    new_columns = []
    for i in range(len(columns)):
        if i == 0:
            new_columns.append(columns[i])
        else:
            if abs(columns[i] - columns[i - 1]) < mz_interval:
                new_columns.append(columns[i - 1])
            else:
                new_columns.append(columns[i])
    mat.columns = new_columns
    mat = mat.T.groupby(mat.columns).sum()
    return mat.T