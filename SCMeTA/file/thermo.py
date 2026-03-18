import pandas as pd
from .format import SCData

def load_txt(path):
    raw = pd.read_table(path)
    raw = raw.drop(columns=["RetentionTime"])
    raw = raw.set_index("Scan")
    return raw

def load_thermo(path, load_range, include_ms2=False, filter_resolution: bool = True) -> pd.DataFrame:

    try:
        import RawFileReaderS
        from tqdm import trange        
        import numpy as np
        import os

        raw = RawFileReaderS.RawFile(path)

        file_name = os.path.splitext(os.path.basename(path))[0]
        scan_nums, masses, intensities = [], [], []
        if load_range is None:
            load_range = (1, raw.n_scans)
        for i in trange(load_range[0], load_range[1] + 1, desc=f"Parsing {file_name}"):
                mzs, ints = raw.scan(i)
                
                data_points_count = len(mzs)
                if data_points_count > 0:
                    scan_nums.append(np.full(data_points_count, i))
                    masses.append(mzs)
                    intensities.append(ints)

        final_df = pd.DataFrame({
            "Mass": np.concatenate(masses),
            "Intensity": np.concatenate(intensities),
            "Scan": np.concatenate(scan_nums)
        })
    except :
        print("Failed to load with RawFileReaderS, trying rolling back to RawFileReader...")
        from RawFileReader.src.RawFileReader import RawFileReader
        from scipy.signal import find_peaks
        reader = RawFileReader(file_path=path)
        data = reader.to_dataframe(include_ms2=include_ms2)
        if filter_resolution:
            peaks, _ = find_peaks(data["Intensity"], distance=16)
            data = data.iloc[peaks]
        data.set_index("Scan", inplace=True)
        data = data.loc[load_range[0]:load_range[1]] if load_range else data
        final_df = data.drop(columns=["RetentionTime"])

    return final_df

def load_thermo_data(name, path, target_attr, load_range, include_ms2=False, filter_resolution: bool = True) -> SCData:
    df = load_thermo(path=path, load_range=load_range, include_ms2=include_ms2, filter_resolution=filter_resolution)
    scdata = SCData(name=name)
    setattr(scdata, target_attr, df)
    return scdata
