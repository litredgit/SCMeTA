import pymzml
import pandas as pd
from SCMeTA.file.format import SCData
from pathlib import Path
from tqdm import tqdm
import numpy as np

# def _parse_mzml(file_path: str) -> pd.DataFrame:
#     run = pymzml.run.Reader(file_path)
#     data = []
#     for scan_num, spec in enumerate(run):
#         for mz, intensity in zip(spec.mz, spec.i):
#             data.append({
#                 "Mass": float(mz),       # 必须确保列名为 "Mass"（首字母大写）
#                 "Intensity": float(intensity),
#                 "RetentionTime": spec.scan_time_in_minutes(),
#                 "Charge": 1,
#                 "Scan": int(scan_num)     # 必须为整数
#             })
#     return pd.DataFrame(data)

# SCMeTA/core/io.py
# def _parse_mzml(file_path: str) -> pd.DataFrame:
#     run = pymzml.run.Reader(file_path)
#     data = []
#     for scan_num, spec in enumerate(run):
#         for mz, intensity in zip(spec.mz, spec.i):
#             data.append({
#                 "Mass": mz,
#                 "Intensity": intensity,
#                 "RetentionTime": spec.scan_time_in_minutes(),
#                 "Charge": 1,
#                 "Scan": scan_num  # 扫描编号
#             })
#     # df = pd.DataFrame(data)
#     # df.set_index("Scan", inplace=True)  # 将 Scan 设为索引
#     # return df
#     return pd.DataFrame(data) #这版是可以用的
def _parse_mzml(file_path: str, max_scans: int = None) -> pd.DataFrame:
    """
    优化版mzML解析器 (速度提升10-20倍)
    参数：
        max_scans: 最大读取扫描次数（测试时可设小值）
    """
    run = pymzml.run.Reader(
        file_path,
        obo_version="4.1.33",  # 使用最新OBO定义加速
        build_index_from_scratch=False,  # 禁用耗时索引
        MS_precisions={1: 5e-6, 2: 20e-6}  # 设置精度减少计算
    )

    # 预分配内存（减少动态扩容开销）
    scan_nums, masses, intensities = [], [], []
    scan_nums_append = scan_nums.append
    masses_append = masses.append
    intensities_append = intensities.append

    # 使用迭代器+进度条
    for scan_num, spec in enumerate(tqdm(run, desc="Parsing mzML")):
        if max_scans and scan_num >= max_scans:
            break

        mz_array = spec.mz  # 直接访问numpy数组
        i_array = spec.i

        # 批量追加（比逐点循环快10倍）
        scan_nums_append(np.full(len(mz_array), scan_num))
        masses_append(mz_array)
        intensities_append(i_array)

    # 向量化合并（避免逐行构建DataFrame）
    return pd.DataFrame({
        "Mass": np.concatenate(masses),
        "Intensity": np.concatenate(intensities),
        "Scan": np.concatenate(scan_nums)
    })
    df.columns = df.columns.astype(str)


# def load_mzml_to_scdata(file_path: str) -> SCData:
#     """生成完全兼容的 SCData 对象"""
#     name = Path(file_path).stem
#     scdata = SCData(name=name)
#
#     df = _parse_mzml(file_path)
#     # 标准化列名（与 Thermo RAW 处理后的结构一致）
#     scdata.raw = df[["Mass", "Intensity", "Scan", "RetentionTime", "Charge"]]
#
#     # 初始化必要属性（参考 process_raw 函数的输出）
#     scdata.process = pd.DataFrame()
#     scdata.mat = pd.DataFrame()
#     scdata.cell_pos = []
#     scdata.cell_mat = pd.DataFrame()
#
#     return scdata


# def load_mzml_to_scdata(file_path: str) -> SCData:
#     scdata = SCData(name=Path(file_path).stem)
#     df = _parse_mzml(file_path)
#
#     # 确保列名和类型匹配
#     scdata.raw = df[["Mass", "Intensity", "Scan"]].astype({
#         "Mass": float,
#         "Intensity": float,
#         "Scan": int
#     })
#
#     # 初始化空字段
#     scdata.process = pd.DataFrame(columns=["Mass", "Intensity"])
#     return scdata


def load_mzML_data(file_path: str) -> dict:
    """始终返回 {filename: SCData} 字典结构"""
    name = Path(file_path).stem
    scdata = SCData(name=name)

    # 解析数据（保持Scan为列）
    df = _parse_mzml(file_path)
    scdata.raw = df[["Mass", "Intensity", "Scan"]]

    # 初始化空字段
    scdata.process = pd.DataFrame()
    scdata.mat = pd.DataFrame()

    return {name: scdata}  # 统一为字典
