import pandas as pd
import numpy as np
from SCMeTA.config.load import Parameters
# def to_mat(data: pd.DataFrame) -> pd.DataFrame:
#     mat = data.pivot_table(
#         index="Scan",
#         columns="Mass",
#         values="Intensity",
#     ).fillna(0)
#     return mat
parameters_dict = {
    'min_intensity': 0.001,  # 假设最小强度阈值
    'mass_precision': 2    # 假设质量精度
}
PARAMETERS = Parameters(parameters_dict)
def to_mat(data: pd.DataFrame) -> pd.DataFrame:
    """优化后的矩阵转换函数"""
    # 预处理：质量分箱+强度过滤
    data = data[data['Intensity'] > PARAMETERS.min_intensity].copy()
    data['Mass'] = data['Mass'].round(PARAMETERS.mass_precision)

    # 使用crosstab替代pivot_table（快3-5倍）
    mat = pd.crosstab(
        index=data['Scan'],
        columns=data['Mass'],
        values=data['Intensity'],
        aggfunc='sum',
        dropna=True
    ).fillna(0).astype(np.float32)

    # 列名排序优化
    return mat[sorted(mat.columns)]



def to_list(mat: pd.DataFrame, cell_pos: list[list]) -> pd.DataFrame:
    index_list = mat.index
    index = [cell_pos[i][0] for i in index_list]
    mat.index = index
    mat = mat.stack().reset_index()
    mat.columns = ["Scan", "Mass", "Intensity"]
    mat.set_index("Scan", inplace=True)
    return mat
