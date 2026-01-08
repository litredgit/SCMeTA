import pandas as pd
from .line import line

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

    line(df=spec, x="Mass", y="Intensity", title=f"Spectra {scan} of {name}", output=output)