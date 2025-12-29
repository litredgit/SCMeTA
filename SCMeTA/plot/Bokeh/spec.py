import pandas as pd
from bokeh.layouts import column
from bokeh.plotting import figure, output_notebook, show
from bokeh.models import WheelZoomTool, PanTool, BoxZoomTool, HoverTool, BasicTickFormatter, Button, CustomJS

def show_spec(
    name: str,
    df: pd.DataFrame,
    scan: int,
    attr: str = "raw",
    output: str = "notebook",
    **kwargs
):
    """Show spectrum plot for a given scan."""
    spec = df.loc[scan]
    # Ensure sorted by scan and compute x range based on Scan values
    if not spec.empty:
        x_start = spec["Mass"].min()
        x_end = spec["Mass"].max()
    else:
        x_start, x_end = 0, 1

    p = figure(
        title=f"Spec {scan} of {name}",
        height=300,
        width=450,
        tools="reset,save",
        toolbar_location="right",
        x_axis_type="auto",
        x_axis_location="below",
        background_fill_color="#efefef",
        x_range=(x_start, x_end)
    )
    p.yaxis[0].formatter = BasicTickFormatter(precision=1)
    p.y_range.start = 0
    p.xgrid.grid_line_color = None
    p.ygrid.grid_line_color = None
    p.background_fill_color = "white"
    p.border_fill_color = "white"

    xwheel = WheelZoomTool(dimensions="width")
    xpan = PanTool(dimensions="width")
    xbox = BoxZoomTool(dimensions="width")
    hover = HoverTool(
    tooltips=[
        ("m/z", "@Mass"),
        ("Intensity", "@Intensity"),
    ],
    mode="vline",
    line_policy="nearest",
    name="hover_main"
)
    button = Button(label="Switch Hover Mode", width=150)

    callback = CustomJS(args=dict(hover=hover, button=button), code="""
        if (hover.mode === "vline") {
            hover.mode = "mouse";
            hover.line_policy = "none";
            button.label = "Switch Hover Mode (mouse)";
        } else {
            hover.mode = "vline";
            hover.line_policy = "nearest";
            button.label = "Switch Hover Mode (vline)";
        }
    """)

    button.js_on_click(callback)

    p.add_tools(xwheel, xpan, xbox, hover)
    p.toolbar.active_scroll = xwheel
    p.toolbar.active_drag = xpan

    p.line("Mass", "Intensity", source=spec, line_color="black")
    p.xaxis.axis_label = "m/z"
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

    # select.line("Scan", "Intensity", source=spec)
    # select.ygrid.grid_line_color = None
    # select.add_tools(rt)

    if output == "notebook":
        output_notebook()
    show(column(p, button))