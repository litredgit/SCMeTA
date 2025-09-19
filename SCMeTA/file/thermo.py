import pandas as pd
from scipy.signal import find_peaks

from .format import SCData
from . import RawFileReader


def load_txt(path):
    raw = pd.read_table(path)
    raw = raw.drop(columns=["RetentionTime"])
    raw = raw.set_index("Scan")
    return raw


def load_thermo(path):
    reader = RawFileReader()
    raw = reader.load(path)
    return raw


def load_thermo(path, include_ms2=False, filter_resolution: bool = True) -> pd.DataFrame:
    reader = RawFileReader(file_path=path)
    data = reader.to_dataframe(include_ms2=include_ms2, filter_threshold=10)
    if filter_resolution:
        peaks, _ = find_peaks(data["Intensity"], distance=16)
        data = data.iloc[peaks]
    data.set_index("Scan", inplace=True)
    data = data.drop(columns=["RetentionTime"])

    return data
