from dataclasses import dataclass, field
from SCMeTA.plot import show_xic
import pandas as pd

@dataclass
class SCData:
    name: str
    raw: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    offset: float = 0
    process: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    mat: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    cell_pos: list = list[list[int]]
    cell_mat: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    cell_count = property(lambda self: self.cell_mat.shape[0])
    database: bool = False

    def clear(self, attributes:list[str] | None = None):
        for attr in attributes:
            attr_value = getattr(self, attr)
            if isinstance(attr_value, (list, dict, set)):
                attr_value.clear()
            elif isinstance(attr_value, pd.DataFrame):
                setattr(self, attr, pd.DataFrame())
                del attr_value
            else:
                setattr(self, attr, None)

    def set_offset(self, offset: float | None):
        if self.offset is None or self.offset == 0:
            self.offset = offset
            self.raw["Mass"] += offset
        elif offset is None or offset == 0:
            pass
        else:
            _offset = offset - self.offset
            self.offset = offset
            self.raw["Mass"] += _offset

    def cut(self, ranges: list[list[int, int]] | list[int, int]):
        if isinstance(ranges[0], int):
            ranges = [ranges]
        intervals = [(s, e) for s, e in ranges]
        slices = [self.raw.loc[s:e] for s, e in intervals]
        self.raw = pd.concat(slices, axis=0)

    def drop(self, ranges: list[list[int, int]] | list[int, int]):
        if isinstance(ranges[0], int):
            ranges = [ranges]
        intervals = [(s, e) for s, e in ranges]
        to_drop = []
        for s, e in intervals:
            to_drop.extend(range(s, e + 1))
        self.raw = self.raw.drop(index=to_drop)

    def xic(self, mz: float):
        return self.process.loc[self.process["Mass"] == mz]

    def show(self, type="xic", attr="raw", refer_mz: float=760.58, output: str="notebook"):
        FUNC = {
            "xic": show_xic
            # "tic": show_tic,
            # "bpc": show_bpc
        }
        FUNC[type](self.raw, refer_mz, attr, output)

    def get_scan(self, scan: int, data_type: str = "raw"):
        if data_type == "raw":
            return self.raw.loc[scan]
        elif data_type == "process":
            return self.process.loc[scan]
        elif data_type == "mat":
            return self.mat.loc[scan]
        elif data_type == "cell_mat":
            for index, sublist in enumerate(self.cell_pos):
                if scan in sublist:
                    return self.cell_mat.loc[index]
        else:
            raise ValueError("data_type must be 'raw', 'process', 'mat' or 'cell_mat'")

    def reset(self):
        self.process = pd.DataFrame()
        self.mat = pd.DataFrame()
        self.cell_pos = []
        self.cell_mat = pd.DataFrame()
        self.raw["Mass"] -= self.offset
        self.offset = 0.0
