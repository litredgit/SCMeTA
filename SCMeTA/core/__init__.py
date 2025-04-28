from .cluster import Process
from .mpl import MplPlot
from .match import SearchDatabase

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # True when type checking, False when running
    from .io import load_mzml, load_data
else:
    # do when running
    def __getattr__(name):
        if name in ("load_mzml", "load_data"):
            from .io import load_mzml, load_data
            return locals()[name]
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")