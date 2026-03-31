import oxion
import pandas as pd
import numpy as np
from SCMeTA.file.format import SCData

def read_file(file_path: str) -> pd.DataFrame:
    raw = oxion.open(file_path, mmap=True)
    all_ms1 = raw.all_ms1_scans(progress=True)
    mz_list, int_list = zip(*all_ms1)
    lengths = np.fromiter((len(m) for m in mz_list), dtype=np.int32)

    df = pd.DataFrame({
        "Mass": np.concatenate(mz_list),
        "Intensity": np.concatenate(int_list),
        "Scan": np.repeat(np.arange(len(lengths)), lengths)
    }).set_index("Scan")

    return df

def gen_scdata(name, path, target_attr) -> SCData:
    scdata = SCData(name=name)
    df = read_file(file_path=path)
    setattr(scdata, target_attr, df)
    return scdata