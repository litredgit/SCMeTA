import pandas as pd

from .format import SCData
from .plugin import RawFileReader


def load_txt(path):
    raw = pd.read_table(path)
    raw = raw.drop(columns=["RetentionTime"])
    raw = raw.set_index("Scan")
    return raw


def load_thermo(path):
    reader = RawFileReader()
    raw = reader.load(path)
    return raw


def load_thermo_data(name, path, target_attr, load_range) -> SCData:
    data = SCData(name)
    if path.lower().endswith(".raw"):
        setattr(data, target_attr, load_thermo(path))
    elif path.lower().endswith(".txt"):
        setattr(data, target_attr, load_txt(path))
    else:
        raise FileNotFoundError("File not supported")
    return data
