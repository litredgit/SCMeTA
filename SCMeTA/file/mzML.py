import os
import pandas as pd
from SCMeTA.file.format import SCData
from tqdm import trange
import numpy as np

def _parse_mzml(file_path: str, load_range: tuple[int, int] | None=None) -> pd.DataFrame:
    """
    mzML reader
    """
    # # pymzml
    # import pymzml
    # run = pymzml.run.Reader(
    #     file_path,
    #     obo_version="4.1.33",  # 使用最新OBO定义加速
    #     build_index_from_scratch=False,  # 禁用耗时索引
    #     MS_precisions={1: 5e-6, 2: 20e-6}  # 设置精度减少计算
    # )
    # total_scans = run.get_spectrum_count()
    # if load_range is None:
    #     load_range = (1, total_scans)

    # # 预分配内存（减少动态扩容开销）
    # scan_nums, masses, intensities, ms_levels = [], [], [], []
    # file_name = os.path.splitext(os.path.basename(file_path))[0]

    # for scan_num in trange(load_range[0], load_range[1]+1, desc=f"Parsing {file_name}"):
    #     spec = run[scan_num]
    #     current_ms_level = spec.ms_level
    #     mz_array = np.around(spec.mz, decimals=3)
    #     i_array = np.around(spec.i, decimals=3)

    #     # 批量追加（比逐点循环快10倍）
    #     scan_nums.append(np.full(len(mz_array), scan_num))
    #     masses.append(mz_array)
    #     intensities.append(i_array)

    # pyteomics
    from pyteomics import mzml
    scan_nums, masses, intensities = [], [], []
    file_name = os.path.splitext(os.path.basename(file_path))[0]

    with mzml.MzML(file_path, build_index=True) as reader:
        total_scans = len(reader)
        if load_range is None:
            load_range = (0, total_scans - 1)

        for i in trange(load_range[0], load_range[1] + 1, desc=f"Parsing {file_name}"):
            spec = reader[i]

            mzs = np.around(spec.get('m/z array', np.array([])), decimals=3)
            ints = np.around(spec.get('intensity array', np.array([])), decimals=3)
            
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

    # 向量化合并（避免逐行构建DataFrame）
    return final_df

def load_mzML_data(name, path, target_attr, load_range) -> SCData:
    scdata = SCData(name=name)

    # load mzML file with Scan as index
    if not path.lower().endswith(".mzml"):
        raise ValueError("File is not a mzML file")
    df = _parse_mzml(file_path=path, load_range=load_range)
    attr = df[["Mass", "Intensity", "Scan"]].set_index("Scan")
    setattr(scdata, target_attr, attr)
    return scdata
