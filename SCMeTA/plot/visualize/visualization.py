import matplotlib.pyplot as plt
import pandas as pd
import io, base64

def to_base64(fig):
    """convert matplotlib figure to base64 PNG"""
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', dpi=150)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')

def plot_eic(df:pd.DataFrame, mz:float=760.58,):
    """plot EIC and return base64 PNG string"""
    eic = df.loc[:, mz].copy()
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(eic.index, eic.values, label=mz)
    ax.set_xlabel("Scan")
    ax.set_ylabel("Intensity")
    ax.set_title("Extracted Ion Chromatogram")
    ax.legend()
    img_b64 = to_base64(fig)
    plt.close(fig)
    return img_b64

def show_eic(df:pd.DataFrame, mz:float=760.58):
    """plot EIC and display in Jupyter; also return the Image object"""
    img_b64 = plot_eic(df, mz)
    from IPython.display import Image, display
    img = Image(data=base64.b64decode(img_b64))
    # Ensure rendering even if this isn't the last expression in the cell
    try:
        display(img)
    except Exception:
        pass
    return img_b64