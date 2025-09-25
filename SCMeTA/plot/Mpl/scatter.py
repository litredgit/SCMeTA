import numpy as np
import pandas as pd
from matplotlib.axes import Axes

# 颜色列表，可根据需要修改颜色种类和数量
COLOR_KEY = [
    "firebrick",
    "darkorange",
    "navy",
    "deepskyblue",
    "violet",
    "pink",
    "green",
    "greenyellow",
    "black",
    "gray",
    "saddlebrown",
    "chocolate",
    "blue",
    "powderblue",
    "yellow",
    "cyan",
    "magenta",
    "olive",
    "teal",
    "indigo",
    "crimson",
    "gold",
    "lime",
    "mediumslateblue",
    "orchid",
    "salmon",
    "seagreen",
    "tan",
    "tomato",
    "wheat",
    "aliceblue",
    "antiquewhite",
    "aqua",
    "aquamarine",
    "azure",
    "beige",
    "bisque",
    "blanchedalmond",
    "blueviolet",
    "brown"
]


def scatter(
    data: np.ndarray | pd.DataFrame,
    ax: Axes,
    title: str,
    cell_range: dict[str, int] | None = None,
    # 散点的大小，可根据需要调整
    point_size: int = 3,
):
    if isinstance(data, np.ndarray):
        if cell_range is None:
            ax.scatter(data[:, 0], data[:, 1], s=point_size, c=COLOR_KEY[0], label="data")
            ax.set_title(title)
        else:
            for i, (key, value) in enumerate(cell_range.items()):
                ax.scatter(
                    data[0: value - 1, 0],
                    data[0: value - 1, 1],
                    s=point_size,
                    c=COLOR_KEY[i],
                    alpha=0.8,
                    edgecolors=COLOR_KEY[i],
                    label=key
                )
                data = data[value:]
    elif isinstance(data, pd.DataFrame):
        ax.scatter(data.iloc[:, 0], data.iloc[:, 1], s=point_size, c=COLOR_KEY[0], label="data")
    else:
        raise TypeError(f"data type {type(data)} is not supported")
    ax.set_title(title, fontsize=16, fontweight='bold')
    # loc 参数指定图例锚点位置，可修改为其他合法值，如 'upper right' 等
    # bbox_to_anchor 参数调整图例相对于坐标轴的位置，可根据实际情况修改
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))


if __name__ == '__main__':
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    # 细胞范围字典，可修改细胞类别和对应的数量
    cell_range = {"A": 10, "B": 20, "C": 30, "D": 40, "E": 50, "F": 60, "G": 70, "H": 80, "I": 90, "J": 100}
    scatter(np.random.random((550, 2)), ax, "test", cell_range)
    # 调整布局以避免图例遮挡图像，可根据需要注释掉此代码查看效果
    plt.tight_layout()
    fig.show()