from bokeh.models import WheelZoomTool, PanTool, BoxZoomTool, HoverTool, BasicTickFormatter, Button, CustomJS, ColumnDataSource
from bokeh.plotting import figure

def scatter_canvas(title:str):
    """
    plot scatter to identify cell events.
    """
    # initialize figure
    p = figure(
        title=title,
        height=300,
        width=450,
        tools="reset,save",
        toolbar_location="right",
        background_fill_color="white",
        border_fill_color="white",
    )
    p.yaxis[0].formatter = BasicTickFormatter(precision=1)
    p.y_range.start = 0
    p.xgrid.visible = False
    p.ygrid.visible = False

    # define tools
    xwheel = WheelZoomTool(dimensions="width")
    xpan = PanTool(dimensions="width")
    xbox = BoxZoomTool(dimensions="width")
    hover = HoverTool(
    tooltips=[
        ("x", "@x"),
        ("y", "@y"),
    ],
    mode="mouse",
    line_policy="none",
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

    return p, button
