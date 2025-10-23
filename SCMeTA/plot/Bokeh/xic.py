import pandas as pd

from bokeh.layouts import column
from bokeh.models import RangeTool
from bokeh.plotting import figure, output_notebook, show
from bokeh.io import push_notebook


def show_xic(
    df: pd.DataFrame,
    refer_mz: float = 760.58,
    attr: str = "raw",
    output: str = "notebook",
    tol: float | None = None,
):
    if attr == "raw":
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
        xic = pd.DataFrame({"Scan": df.index, "Intensity": intensity})

    # Ensure sorted by scan and compute x range based on Scan values
    if not xic.empty:
        x_start = xic.index.min()
        x_end = xic.index.max()
    else:
        x_start, x_end = 0, 1

    p = figure(
        title=f"EIC {refer_mz}",
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

    select = figure(
        title="Drag the middle and edges of the selection box to change the range above",
        height=130,
        width=800,
        y_range=p.y_range,
        x_axis_type="auto",
        y_axis_type=None,
        tools="",
        toolbar_location="right",
        background_fill_color="#efefef",
    )

    rt = RangeTool(x_range=p.x_range)
    rt.overlay.fill_color = "navy"
    rt.overlay.fill_alpha = 0.2

    select.line("Scan", "Intensity", source=xic)
    select.ygrid.grid_line_color = None
    select.add_tools(rt)

    if output == "notebook":
        output_notebook()
        show(column(p, select))
    elif output == "ide":
        output_notebook()
        handle = show(column(p, select), notebook_handle=True)
        push_notebook(handle=handle)
    else:
        show(column(p, select))
