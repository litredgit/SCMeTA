import pandas as pd

from .format import SCData


def load_process_data(name, path, target_attr = "cell_mat", load_range: tuple[int, int] | None = None) -> SCData:
    process = pd.read_csv(path, index_col=0)
    data = SCData(name)
    setattr(data, target_attr, process)
    return data
