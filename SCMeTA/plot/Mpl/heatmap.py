import numpy as np
import pandas as pd

from matplotlib.cm import ScalarMappable
import matplotlib.pyplot as plt
from matplotlib.figure import Figure, Axes
import matplotlib.ticker as ticker

from SCMeTA.method import combine_mat

CMAP_KEY = {
    "viridis": plt.cm.viridis,
    "plasma": plt.cm.plasma,
    "inferno": plt.cm.inferno,
    "magma": plt.cm.magma,
    "jet": plt.cm.jet,
    "bwr": plt.cm.bwr,
    "seismic": plt.cm.seismic,
}

LOG_KEY = {
    "LOG10": np.log10,
    "LOG2": np.log2,
    "LOG": np.log,
    "NO_LOG": lambda x: x
}


def heatmap(
    mat_list: dict[str, pd.DataFrame],
    cell_range: dict[str, int],
    ax: Axes,
    fig: Figure,
    color_map="jet",
    func: str = "LOG2",
    title: str = "Heatmap",
):
    mat = combine_mat(mat_list.values()).fillna(0.00001)
    if (func == "NO_LOG"):
        mat = mat
    else:
        mat = mat.apply(LOG_KEY[func])
    mat = mat.T
    im = ax.imshow(
        mat, cmap=CMAP_KEY[color_map], aspect="auto"
    )  # save AxesImages from imshow
    mass = mat.index.values

    cell_numb = list(cell_range.values())
    cell_name = list(cell_range.keys())

    # name x with cells
    new_cell_numb = [0] * len(cell_numb)
    for a in range(len(cell_numb)):
        if a == 0:
            new_cell_numb[a] = 0
        else:
            new_cell_numb[a] = new_cell_numb[a - 1] + cell_numb[a - 1]
    for b in range(len(cell_numb)):
        new_cell_numb[b] = new_cell_numb[b] + int(cell_numb[b] / 2)

    ax.set_xlabel("Cell")
    ax.set_ylabel("Mass")
    ax.set_title(title)
    fig.colorbar(im, ax=ax)  # set color bar with AxesImage

    ax.xaxis.set_major_locator(ticker.FixedLocator(new_cell_numb))  # set x location
    ax.xaxis.set_major_formatter(ticker.FixedFormatter(cell_name))  # set x name
    ax.set_yticks(np.arange(len(mass)), labels=mass)  # set y location and name
    ax.yaxis.set_major_locator(ticker.MultipleLocator(5))  # show y label for every 5 y label

    ax.set_xticklabels(cell_name, rotation=45, ha='right') # rotate x label
