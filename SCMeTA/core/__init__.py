from .cluster import Process
from .mpl import MplPlot
from .match import SearchDatabase

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # 仅用于类型检查
    from .io import load_mzml, load_data
else:
    # 运行时动态导入
    def __getattr__(name):
        if name in ("load_mzml", "load_data"):
            from .io import load_mzml, load_data
            return locals()[name]
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")