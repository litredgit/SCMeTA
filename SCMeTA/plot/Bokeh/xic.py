import pandas as pd

from bokeh.layouts import column
from bokeh.models import RangeTool
from bokeh.plotting import figure, output_notebook, show

def show_xic(
    name: str,
    df: pd.DataFrame,
    refer_mz: float = 760.58,
    attr: str = "raw",
    output: str = "notebook",
    tol: float | None = None,
):
    if attr == "raw" or attr == "process":
        if tol is None:
            xic = df[df["Mass"] == refer_mz]
        else:
            lower, upper = refer_mz - tol, refer_mz + tol
            xic = df[(df["Mass"] >= lower) & (df["Mass"] <= upper)]
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

    # Ensure sorted by scan and compute x range based on Scan values
    if not xic.empty:
        x_start = xic.index.min()
        x_end = xic.index.max()
    else:
        x_start, x_end = 0, 1

    p = figure(
        title=f"EIC {refer_mz} of {name}",
        height=300,
        width=800,
        tools="hover,pan,wheel_zoom,box_zoom,reset,save",
        toolbar_location="right",
        x_axis_type="auto",
        x_axis_location="above",
        background_fill_color="#efefef",
        x_range=(x_start, x_end),
    )
    p.line("Scan", "Intensity", source=xic)
    p.xaxis.axis_label = "Scan"
    p.yaxis.axis_label = "Intensity"

    # select = figure(
    #     title="Drag the middle and edges of the selection box to change the range above",
    #     height=130,
    #     width=800,
    #     y_range=p.y_range,
    #     x_axis_type="auto",
    #     y_axis_type=None,
    #     tools="",
    #     toolbar_location="right",
    #     background_fill_color="#efefef",
    # )

    # rt = RangeTool(x_range=p.x_range)
    # rt.overlay.fill_color = "navy"
    # rt.overlay.fill_alpha = 0.2

    # select.line("Scan", "Intensity", source=xic)
    # select.ygrid.grid_line_color = None
    # select.add_tools(rt)

    if output == "notebook":
        output_notebook()
    show(column(p))


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

    # Determine x-range from scan index
    if not tic.empty:
        x_start = tic.index.min()
        x_end = tic.index.max()
    else:
        x_start, x_end = 0, 1

    p = figure(
        title=f"TIC of {name}",
        height=300,
        width=800,
        tools="hover,pan,wheel_zoom,box_zoom,reset,save",
        toolbar_location="right",
        x_axis_type="auto",
        x_axis_location="above",
        background_fill_color="#efefef",
        x_range=(x_start, x_end),
    )
    p.line("Scan", "Intensity", source=tic)
    p.xaxis.axis_label = "Scan"
    p.yaxis.axis_label = "Intensity"

    if output == "notebook":
        output_notebook()
    show(column(p))


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

    if not bpc.empty:
        x_start = bpc.index.min()
        x_end = bpc.index.max()
    else:
        x_start, x_end = 0, 1

    p = figure(
        title=f"BPC of {name}",
        height=300,
        width=800,
        tools="hover,pan,wheel_zoom,box_zoom,reset,save",
        toolbar_location="right",
        x_axis_type="auto",
        x_axis_location="above",
        background_fill_color="#efefef",
        x_range=(x_start, x_end),
    )
    p.line("Scan", "Intensity", source=bpc)
    p.xaxis.axis_label = "Scan"
    p.yaxis.axis_label = "Intensity"

    if output == "notebook":
        output_notebook()
    show(column(p))
