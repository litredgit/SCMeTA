import pandas as pd
import numpy as np

def sum_df(df: pd.DataFrame, scan: int) -> pd.DataFrame:
    # sum intensities of the same mass
    df = df.groupby("Mass").sum().reset_index()
    df.insert(0, "Scan", scan)
    df = df.set_index("Scan")
    return df

def combine_peaks(raw: pd.DataFrame, res_mz: float = 0.01) -> pd.DataFrame:
    # round mass to resolution res_mz
    raw["Mass"] = np.floor(raw["Mass"] / res_mz) * res_mz
    
    # combine peaks with the same mass
    scans = raw.index.unique()
    temp = [sum_df(raw.loc[[scan]], scan) for scan in scans]
    return pd.concat(temp)