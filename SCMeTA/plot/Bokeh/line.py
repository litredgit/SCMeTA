import pandas as pd
from bokeh.layouts import column
from bokeh.models import WheelZoomTool, PanTool, BoxZoomTool, HoverTool, BasicTickFormatter, Button, CustomJS, ColumnDataSource
from bokeh.plotting import figure, output_notebook, show

def line(df: pd.DataFrame, x:str, y:str, title:str | None = None, output:str = "notebook"):
    """
    plot line.
    """
    # Ensure sorted by scan and compute x range based on Scan values
    if not df.empty:
        if x == df.index.name:
            x_start = df.index.min()
            x_end = df.index.max()
        else:
            x_start = df[x].min()
            x_end = df[x].max()

        # xic.index = xic.index.astype(float) / 20
    else:
        raise ValueError("input empty dataframe")
    
    # initialize figure
    p = figure(
        title=title,
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

    # define tools
    xwheel = WheelZoomTool(dimensions="width")
    xpan = PanTool(dimensions="width")
    xbox = BoxZoomTool(dimensions="width")
    hover = HoverTool(
    tooltips=[
        ("x", "@x"),
        ("y", "@y"),
    ],
    mode="vline",
    line_policy="nearest",
    name="hover_main"
)
    # define switch button within hover mouse and vline
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

    # plot line
    source = ColumnDataSource({
        "x": df.index.values if x == df.index.name else df[x].values,
        "y": df[y].values
    })
    p.line("x", "y", source=source, line_color="black")
    p.xaxis.axis_label = x
    p.yaxis.axis_label = y

    if output == "notebook":
        output_notebook()
    show(column(p, button))