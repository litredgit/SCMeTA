import pandas as pd
import numpy as np
from .line import line
from .scatter import scatter_canvas

def show_xic(
    name: str,
    df: pd.DataFrame,
    refer_mz: float = 760.58,
    attr: str = "raw",
    output: str = "notebook",
    tol: float | None = None,
    **kwargs
):
    """Plot Extracted Ion Chromatogram.

    Supports two shapes of input:
    - raw/process table with columns including 'Scan' and 'Intensity'.
    - pivoted matrix with scan index and m/z columns (floats/numbers).
    """
    # extract source data
    if attr == "raw" or attr == "process":
        if tol is None:
            xic = df[df["Mass"] == refer_mz]
        else:
            lower, upper = refer_mz - tol, refer_mz + tol
            xic = df[(df["Mass"] >= lower) & (df["Mass"] <= upper)]

        # reindex
        full_index = range(df.index.min(), df.index.max() + 1)
        xic = xic.reindex(full_index).fillna(0)
        xic.index.name = "Scan"
        
    else:
        # pivoted matrix: index is scan, columns are m/z values (float). When tol provided,
        # sum all columns within the window; otherwise use exact column.
        if tol is None:
            intensity = df[refer_mz]
        else:
            cols = [
                c
                for c in df.columns
                if isinstance(c, (int, float)) and abs(float(c) - refer_mz) <= tol
            ]
            if cols:
                intensity = df[cols].sum(axis=1)
            else:
                # no column within tolerance; create zeros to keep shape
                intensity = pd.Series(0, index=df.index, name="Intensity")
        xic = intensity.to_frame().set_index(df.index)
        xic.index.name = "Scan"
        xic.columns = ["Intensity"]
     
    # plot line
    line(df=xic, x="Scan", y="Intensity", title=f"EIC {refer_mz} of {name}", output=output)

def show_cell_event(
    name: str,
    df: pd.DataFrame,
    cell_pos: list,
    refer_mz: float = 760.58,
    output: str = "notebook",
    **kwargs
):
    """Plot chromatogram highlighting cell events.

    Args:
        name (str): Name of the sample or file.
        df (pd.DataFrame): DataFrame with 'Scan' index and m/z columns.
        cell_pos (list): List of tuples indicating start and end scan of cell events.
        refer_mz (float, optional): Reference m/z value. Defaults to 760.58.
        output (str, optional): Output mode for Bokeh. Defaults to "notebook".
    """
    from bokeh.plotting import output_notebook, show
    from bokeh.models import ColumnDataSource
    from bokeh.layouts import column

    # Extract cell event points
    scan = df.index.to_numpy()
    is_event = np.zeros(len(df), dtype=bool)

    for start, end in cell_pos:
        is_event |= (scan >= start) & (scan <= end)
    df_event = df[is_event]
    df_other = df[~is_event]

    # make source data
    source_event = ColumnDataSource(data={
        "x": df_event.index,
        "y": df_event[refer_mz]
    })
    source_other = ColumnDataSource(data={
        "x": df_other.index,
        "y": df_other[refer_mz]
    })

    # plot scatter
    p, hover_button = scatter_canvas(title=f"EIC scatter (m/z = {refer_mz}) of {name}")
    p.scatter(
        source=source_other,
        x="x",
        y="y",
        size=4,
        color="gray",
        alpha=0.35,
        legend_label="Other"
    )
    p.scatter(
        source=source_event,
        x="x",
        y="y",
        size=4,
        color="red",
        alpha=1.0,
        legend_label="Cell Event"
    )
    if output == "notebook":
        output_notebook()
    show(column(p, hover_button))

def show_tic(
    name: str,
    df: pd.DataFrame,
    attr: str = "raw",
    output: str = "notebook",
    **kwargs
):
    """Plot Total Ion Chromatogram (sum intensity per scan).

    Supports two shapes of input:
    - raw/process table with columns including 'Scan' and 'Intensity'.
    - pivoted matrix with scan index and m/z columns (floats/numbers).
    """
    if attr == "raw" or attr == "process":
        # Sum intensity per scan
        if "Scan" in df.columns and "Intensity" in df.columns:
            intensity = df.groupby("Scan")["Intensity"].sum().sort_index()
        else:
            # Fallback: try using index as scan if it is named/structured
            if df.index.name == "Scan" and "Intensity" in df.columns:
                intensity = df["Intensity"].groupby(level=0).sum().sort_index()
            else:
                # Unable to infer, create an empty series to avoid crashing
                intensity = pd.Series(dtype=float, name="Intensity")
    else:
        # Pivoted: sum across numeric columns (m/z) per scan
        numeric_cols = [
            c for c in df.columns if isinstance(c, (int, float))
        ]
        if not numeric_cols:
            numeric_cols = df.select_dtypes(include="number").columns.tolist()
        if numeric_cols:
            intensity = df[numeric_cols].sum(axis=1)
        else:
            intensity = pd.Series(0, index=df.index, name="Intensity")

    tic = intensity.to_frame(name="Intensity")
    tic.index.name = "Scan"

    line(df=tic, x="Scan", y="Intensity", title=f"TIC of {name}", output=output)


def show_bpc(
    name: str,
    df: pd.DataFrame,
    attr: str = "raw",
    output: str = "notebook",
    **kwargs
):
    """Plot Base Peak Chromatogram (max intensity per scan).

    Supports two shapes of input:
    - raw/process table with columns including 'Scan' and 'Intensity'.
    - pivoted matrix with scan index and m/z columns (floats/numbers).
    """
    if attr == "raw" or attr == "process":
        # Max intensity per scan
        if "Scan" in df.columns and "Intensity" in df.columns:
            intensity = df.groupby("Scan")["Intensity"].max().sort_index()
        else:
            if df.index.name == "Scan" and "Intensity" in df.columns:
                intensity = df["Intensity"].groupby(level=0).max().sort_index()
            else:
                intensity = pd.Series(dtype=float, name="Intensity")
    else:
        # Pivoted: max across numeric columns per scan
        numeric_cols = [
            c for c in df.columns if isinstance(c, (int, float))
        ]
        if not numeric_cols:
            numeric_cols = df.select_dtypes(include="number").columns.tolist()
        if numeric_cols:
            intensity = df[numeric_cols].max(axis=1)
        else:
            intensity = pd.Series(0, index=df.index, name="Intensity")

    bpc = intensity.to_frame(name="Intensity")
    bpc.index.name = "Scan"

    line(df=bpc, x="Scan", y="Intensity", title=f"BPC of {name}", output=output)
