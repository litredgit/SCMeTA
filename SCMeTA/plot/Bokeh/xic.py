import pandas as pd
from .line import line

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
