import os

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure
from matplotlib.axes import Axes

from SCMeTA.plot.Mpl import scatter, heatmap, line, radar, volcano, bar, box
from SCMeTA.method import round_rows, round_columns, k_w_test
from SCMeTA.method.machine_learning import discriminate, to_mat, kmeans
from SCMeTA.file import SCData
from SCMeTA.config import setup_logger

plt.rcParams["font.family"] = "Arial"
plt.rcParams["figure.dpi"] = 300

FIGURE_SIZE = {
    "heatmap": (7, 5),
    "scatter": (5, 5),
    "line": (10, 5),
}

class MplPlot:
    def __init__(self, path: str |None = None):
        self.__data: dict[str, SCData] | None = None
        self.__mat: dict[str, pd.DataFrame] = {}
        self.__cell_range: dict[str, int] = {}
        self.path = path
    def __read_csv(self, path: str):
        mat = pd.read_csv(path, index_col=0)
        mat.columns = mat.columns.astype(float)
        name = os.path.basename(path).split(".")[0]
        self.__mat[name] = mat

    def sort_loaded_data(self, sort_key: str = "alphabetical"):
        """
        Sort loaded data 
        Args:
            sort_key: sort key, default is "alphabetical"
        """
        self.logger.info(f"Sort data by {sort_key}")
        print("original order:\n", self.__mat.keys())
        if sort_key == "alphabetical":
            self.__mat = dict(sorted(self.__mat.items(), key=lambda x: x[0]))
        else:
            sorted_list = []
            if sort_key == "manual":
                sorted_list = input("Please input the name for each file by order, separated by comma: ")
            else:
                sorted_list = sorted(self.__mat.keys(), key=lambda x: sort_key(x))
            try:
                self.__mat = {name: self.__mat[name] for name in sorted_list}
            except KeyError as e:
                raise KeyError(f"KeyError: {e}. Please check the sort key")
        print("sorted order:\n", self.__mat.keys())

    def load(
            self, 
            data: dict[str, SCData] | None = None, 
            path: str | None = None, 
            sort_key: str="alphabetical"
            ):
        if data or path:
            pass
        else:
            raise ValueError("Please provide data or path to load data")
        
        # Load Cell Mat from dir or file
        if data is not None:
            self.__data = data
            for name, ms_data in data.items():
                self.__mat[name] = ms_data.cell_mat
            self.logger = setup_logger(log_file=os.getcwd() + "/plt.log")
        
        if path is not None:
            if os.path.isdir(path):
                files = os.listdir(path)
                # read
                for file in files:
                    if file.endswith(".csv"):
                        self.__read_csv(os.path.join(path, file))
            elif os.path.isfile(path):
                    self.__read_csv(path)
            self.logger = setup_logger(log_file=os.path.dirname(path) + "/plt.log")
        # sort loaded data
        self.sort_loaded_data(sort_key=sort_key)
        self.__cell_range = {key: mat.shape[0] for key, mat in self.__mat.items()}


    @staticmethod
    def init_plot(rows: int, cols: int, figure_type: str) -> tuple[Figure, Axes]:
        """
        Initialize a matplotlib figure and axes
        Args:
            rows: number of rows
            cols: number of columns
            figure_type: figure type
        Returns:
            fig: matplotlib figure
            ax: matplotlib axes
        """
        figsize = FIGURE_SIZE[figure_type]
        fig, ax = plt.subplots(
            nrows=rows,
            ncols=cols,
            figsize=(figsize[0] * cols, figsize[1] * rows),
            dpi=100,
        )
        return fig, ax

    def pca(self, ax=None, n_components: int = 2, save: bool=False, save_path: str | None=None, dpi: int | None = None):
        full = discriminate(self.__mat, method="pca", n_components=n_components)
        if ax is None:
            fig, ax = self.init_plot(1, 1, "scatter")
        scatter(data=full, cell_range=self.__cell_range, ax=ax, title="PCA")
        if save is True:
            self.save_plot(save_path=save_path, dpi=dpi, name="pca")
        return to_mat(full, self.__cell_range)

    def tsne(self, ax=None, n_components: int = 2, save: bool=False, save_path: str | None=None, dpi: int | None = None):
        full = discriminate(self.__mat, method="tsne", n_components=n_components)
        if ax is None:
            fig, ax = self.init_plot(1, 1, "scatter")
        scatter(data=full, cell_range=self.__cell_range, ax=ax, title="t-SNE")
        if save is True:
            self.save_plot(save_path=save_path, dpi=dpi, name="tsne")
        return to_mat(full, self.__cell_range)

    def umap(self, ax=None, n_components: int = 2, save: bool=False, save_path: str | None=None, dpi: int | None = None):
        full = discriminate(self.__mat, method="umap", n_components=n_components)
        if ax is None:
            fig, ax = self.init_plot(1, 1, "scatter")
        scatter(data=full, cell_range=self.__cell_range, ax=ax, title="UMAP")
        if save is True:
            self.save_plot(save_path=save_path, dpi=dpi, name="umap")
        return to_mat(full, self.__cell_range)

    def scatter_select(self, method: list[str] | None = None, n_components: int = 2):
        if method is None:
            method = ["pca", "tsne", "umap"]
        fig, ax = self.init_plot(1, len(method), "scatter")
        for i, m in enumerate(method):
            if m == "pca":
                self.pca(ax[i], n_components)
            elif m == "tsne":
                self.tsne(ax[i], n_components)
            elif m == "umap":
                self.umap(ax[i], n_components)
            else:
                raise ValueError(f"method {m} is not supported")

    def heatmap(self, ax=None, fig=None, save: bool=False, save_path: str | None=None, dpi: int | None = None, **kwargs):
        if ax is None:
            fig, ax = self.init_plot(1, 1, "heatmap")
        heatmap(
            self.__mat,
            cell_range=self.__cell_range,
            ax=ax,
            fig=fig,
            title="Heatmap",
            **kwargs,
        )
        if save is True:
            self.save_plot(save_path=save_path, dpi=dpi, name="heatmap")

    def volcano(self, name1: str, name2: str, ax=None, save: bool=False, save_path: str | None=None, dpi: int | None = None, **kwargs):
        mat1 = self.__mat[name1]
        mat2 = self.__mat[name2]
        if ax is None:
            fig, ax = self.init_plot(1, 1, "scatter")
        volcano(mat1, mat2, name1, name2, ax=ax, **kwargs)
        if save is True:
            self.save_plot(save_path=save_path, dpi=dpi, name="volcano")

    def box(self, ax=None, method: str = "tsne", save: bool=False, save_path: str | None=None, dpi: int | None = None):
        if ax is None:
            fig, ax = self.init_plot(1, 1, "scatter")
        full_data = discriminate(self.__mat, method=method, n_components=1)
        dict_data = to_mat(full_data, self.__cell_range, n_components=1)
        h_statistic, p_value = k_w_test(dict_data)
        box(dict_data, ax=ax, h_statistic=h_statistic, p_value=p_value)
        if save is True:
            self.save_plot(save_path=save_path, dpi=dpi, name="box")
        return dict_data
    
    def k_means(self, n_clusters: int = 2):
        full = kmeans(mat, n_clusters=n_clusters)
        fig, ax = self.init_plot(1, 1, "scatter")
        scatter(data=full, cell_range=self.__cell_range, ax=ax, title="K-Means")
        return to_mat(full, self.__cell_range)

    # def radar(self, ax=None, mz_list: pd.DataFrame | None = None):
    #     if ax is None:
    #         fig, ax = self.init_plot(1, 1)
    #         radar(self.__mat, ax, self.__cell_range, title="Radar")
    #         return fig
    #     radar(self.__mat, ax, self.__cell_range, title="Radar")

    def ms_compare(self, name: str):
        fig, ax = self.init_plot(2, 1, "line")
        ms_data = self.__data[name]
        scan_list = ms_data.cell_pos
        scan = int(input(f"Please Select a scan from:\n {scan_list}"))
        if scan not in scan_list:
            self.logger.warning(f"Scan {scan} is not in the scan list")
            raise ValueError(f"Scan {scan} is not in the scan list")
        line1 = round_columns(ms_data.get_scan(scan, data_type="raw"), axis=0)
        line2 = ms_data.get_scan(scan, data_type="cell_mat")
        full = pd.concat([line1, line2], axis=1).fillna(0)
        full.columns = ["Raw", "Process"]
        bar(full, ax=ax)
        return full

    def cell_compare(self, name: str):
        fig, ax = self.init_plot(2, 1, "line")
        ms_data = self.__data[name]
        cell_list = ms_data.cell_mat.index.tolist()
        cell1 = int(input(f"Please Select a cell from:\n {cell_list}"))
        cell2 = int(input(f"Please Select a cell from:\n {cell_list}"))
        if cell1 not in cell_list or cell2 not in cell_list:
            self.logger.warning(f"Cell {cell1} or {cell2} is not in the cell list")
            raise ValueError(f"Cell {cell1} or {cell2} is not in the cell list")
        line1 = ms_data.cell_mat.loc[cell1]
        line2 = ms_data.cell_mat.loc[cell2]
        full = pd.concat([line1, line2], axis=1).fillna(0)
        full.columns = [cell1, cell2]
        bar(full, ax=ax)
        return full

    def save_plot(
            self, 
            save_path: str | None = None, 
            dpi: int | None = None,
            name: str = "figure"
            ):
        if save_path is None:
            if self.path is None:
                raise ValueError("Please provide a path to save the figure")
            save_path = self.path + "/figure"
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        if dpi is None:
            dpi = 300
        plt.savefig(save_path + f"/{name}.jpg", dpi=dpi, bbox_inches='tight')
        self.logger.info(f"{name}.jpg saved in {save_path}/, dpi={dpi}")