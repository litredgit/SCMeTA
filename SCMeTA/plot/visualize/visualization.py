import matplotlib.pyplot as plt
import pandas as pd
import io, base64

def to_base64(fig):
    """convert matplotlib figure to base64 PNG"""
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', dpi=150)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')

def plot_eic(df:pd.DataFrame, mz:float=760.58):
    """plot EIC and return base64 PNG string"""
    eic = df.loc[:, mz].copy()
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(eic.index, eic.values, label="EIC")
    ax.set_xlabel("Time")
    ax.set_ylabel("Intensity")
    ax.set_title("Extracted Ion Chromatogram")
    ax.legend()
    img_b64 = to_base64(fig)
    plt.close(fig)
    return img_b64