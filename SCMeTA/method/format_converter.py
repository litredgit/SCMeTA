import pandas as pd


def convert_format(indict: dict[str, pd.DataFrame], type: str = "MetaboAnalyst") -> pd.DataFrame:
    """
    Convert the format of the input dictionary to a DataFrames in targeted format.
    :param indict: Input dictionary with keys as file names and values as DataFrames.
    :return: Dictionary with keys as file names and values as DataFrames.
    """
    typedict={
        "MetaboAnalyst": toMetaboAnalyst,
    }
    if type not in typedict:
        raise ValueError(f"Unsupported type: {type}. Supported types are: {list(typedict.keys())}")
    else:
        indict = dict(sorted(indict.items(), key=lambda x: x[0]))
        outdf = typedict[type](indict)
    return outdf


def toMetaboAnalyst(indict: dict) -> pd.DataFrame:
    dflist = []
    for key, df in indict.items():
        # insert the first column
        df.insert(0, 'Label', key)
        # update index name
        df.index.name = 'Sample'
        # update index
        df.index = key + df.index.astype(str)
        dflist.append(df)
    return pd.concat(dflist, axis=0, join='outer')