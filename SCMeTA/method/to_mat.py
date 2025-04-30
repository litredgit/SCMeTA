import pandas as pd
import numpy as np

def to_mat(data: pd.DataFrame, min_intensity: float = 0.001) -> pd.DataFrame:

    # filter out low intensity values
    data = data[data['Intensity'] > min_intensity].copy()

    # convert data format
    mat = pd.crosstab( 
        index=data.index,
        columns=data['Mass'],
        values=data['Intensity'],
        aggfunc='sum',
        dropna=True
    ).fillna(0).astype(np.float32)

    # sort columns
    return mat[sorted(mat.columns)]



def to_list(mat: pd.DataFrame, cell_pos: list[list]) -> pd.DataFrame:
    index_list = mat.index
    index = [cell_pos[i][0] for i in index_list]
    mat.index = index
    mat = mat.stack().reset_index()
    mat.columns = ["Scan", "Mass", "Intensity"]
    mat.set_index("Scan", inplace=True)
    return mat
