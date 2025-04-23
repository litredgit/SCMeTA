import pandas as pd
import numpy as np
from scipy import stats
from matplotlib.figure import Axes


def clean_df(
    df0: pd.DataFrame, df1: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, list]:
    df0.dropna(how="all", inplace=True, axis=1)
    df1.dropna(how="all", inplace=True, axis=1)
    col_0 = df0.columns.tolist()
    col_1 = df1.columns.tolist()
    columns = [c for c in col_0 if c in col_1]
    df0 = df0.loc[:, columns].fillna(0)
    df1 = df1.loc[:, columns].fillna(0)
    return df0, df1, columns


def volcano(
    mat1: pd.DataFrame,
    mat2: pd.DataFrame,
    name1: str,
    name2: str,
    ax: Axes,
    x_threshold: float = 2.0,
    y_threshold: float = 10,
):
    df0, df1, columns = clean_df(mat1, mat2)

    _list0 = np.log2((df0.mean(axis=0) / df1.mean(axis=0)).tolist())
    _list1 = []
    for i in columns:
        _, p = stats.ttest_ind(df0.loc[:, i], df1.loc[:, i], equal_var=stats.levene(df0.loc[:, i], df1.loc[:, i])[1] > 0.05)
        _list1.append(-np.log(p))

    df_volcano = pd.DataFrame({"Mass": columns, "x": _list0, "y": _list1})

    df_volcano["group"] = "black"
    df_volcano.loc[
        (df_volcano.x > x_threshold) & (df_volcano.y > y_threshold), "group"
    ] = "tab:red"
    df_volcano.loc[
        (df_volcano.x < -x_threshold) & (df_volcano.y > y_threshold), "group"
    ] = "tab:blue"
    df_volcano.loc[df_volcano.y < y_threshold, "group"] = "dimgrey"

    x_min = df_volcano["x"].min()
    x_max = df_volcano["x"].max()
    y_min = df_volcano["y"].min()
    y_max = df_volcano["y"].max()
    padding_x = 0.1 * (x_max - x_min)
    padding_y = 0.1 * (y_max - y_min)
    x_min -= padding_x
    x_max += padding_x
    y_min -= padding_y
    y_max += padding_y

    ax.set(xlim=(x_min, x_max), ylim=(y_min, y_max), title="")
    ax.scatter(df_volcano["x"], df_volcano["y"], s=5, c=df_volcano["group"])
    ax.set_xlabel("log2(fold change)")
    ax.set_ylabel("-log10(P)")
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)

    ax.vlines(
        -x_threshold, y_min, y_max, color="dimgrey", linestyle="dashed", linewidth=1
    )
    ax.vlines(
        x_threshold, y_min, y_max, color="dimgrey", linestyle="dashed", linewidth=1
    )
    ax.hlines(
        y_threshold, x_min, x_max, color="dimgrey", linestyle="dashed", linewidth=1
    )

    x_range = x_max - x_min
    step_x = np.ceil(x_range / 6)
    start_x = np.floor(x_min / step_x) * step_x
    x_ticks = np.arange(start_x, start_x + 7 * step_x, step_x).astype(int)
    ax.set_xticks(x_ticks)

    # 修改y轴刻度部分
    y_min_ceil = np.ceil(y_min / 10) * 10
    y_max_floor = np.floor(y_max / 10) * 10
    y_range = y_max_floor - y_min_ceil
    num_ticks = 5  # 选择展示的刻度数量
    step_y = y_range / (num_ticks - 1)
    y_ticks = np.arange(0, num_ticks) * step_y + y_min_ceil
    y_ticks = np.round(y_ticks / 10) * 10  # 确保刻度是10的倍数
    ax.set_yticks(y_ticks)

    index0 = df_volcano[df_volcano.group == "tab:red"].sort_values(by="y", ascending=False).index[:10].tolist()
    index1 = df_volcano[df_volcano.group == "tab:blue"].sort_values(by="y", ascending=False).index[:10].tolist()
    for i in index0:
        ax.annotate(
            df_volcano.loc[i, "Mass"],
            xy=(df_volcano.loc[i, "x"], df_volcano.loc[i, "y"]),
            xytext=(df_volcano.loc[i, "x"] + 0.02, df_volcano.loc[i, "y"] + 0.02),
            size=10,
        )
    for i in index1:
        ax.annotate(
            df_volcano.loc[i, "Mass"],
            xy=(df_volcano.loc[i, "x"], df_volcano.loc[i, "y"]),
            xytext=(df_volcano.loc[i, "x"] + 0.02, df_volcano.loc[i, "y"] + 0.02),
            size=10,
        )

    title = f"{name1} and {name2}"
    ax.set_title(title)