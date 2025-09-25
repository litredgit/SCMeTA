from configparser import ConfigParser
from pathlib import Path
import pandas as pd

from SCMeTA.config.default import DEFAULT_CONFIG

import platform

SYSTEM = platform.system()

def get_config(CONFIG_PATH: Path = None):
    parser = ConfigParser()
    parser.read(CONFIG_PATH)
    return parser

class Parameters:
    def __init__(self, parameters):
        self.parameters = parameters

    def __getattr__(self, item):
        value = self.parameters[item]
        if isinstance(value, str):
            if value.isdigit():
                value = int(value)
            elif value.replace(".", "", 1).isdigit():
                value = float(value)
            elif value.lower() == "true":
                value = True
            elif value.lower() == "false":
                value = False
            elif value.lower() == "none":
                value = None
        else:
            raise ValueError("Value not str, check ~/.scmeta/config.ini")
        return value

    def __getitem__(self, item):
        return self.__getattr__(item)
    
def setup_config(config_path: str = None, renew: bool = False):
    """
    Setup configuration file.

    Parameters
    ----------
    path : str, optional
        Path to the configuration file. If None, use default path (~/.scmeta/config.ini). If "cwd", use current working directory (.scmeta/config.ini). If a directory is provided, use that directory to store config.ini. Default is None.
    renew : bool, optional
        Whether to renew the configuration file. Default is False.
    """
    # set config path and parent directory
    if not config_path:
        CONFIG_DIR = Path("~/.scmeta").expanduser()
        CONFIG_PATH = CONFIG_DIR / "config.ini"
    elif config_path == "cwd":
        CONFIG_DIR = Path.cwd() / ".scmeta"
        CONFIG_PATH = CONFIG_DIR / "config.ini"
    else:
        CONFIG_PATH = Path(config_path).expanduser()
        if CONFIG_PATH.is_dir():
            CONFIG_DIR = CONFIG_PATH
            CONFIG_PATH = CONFIG_DIR / "config.ini"
        else:
            CONFIG_DIR = CONFIG_PATH.parent
    # create config file if not exists
    if (not CONFIG_PATH.exists()) or renew:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        with open(CONFIG_PATH, "w") as f:
            f.write(DEFAULT_CONFIG)
    # load config file
    config = get_config(CONFIG_PATH=CONFIG_PATH)
    PARAMETERS = Parameters(config["PARAMETERS"])
    METABOLITE = Parameters(config["METABOLITE"])

    try:
        INCLUDE_LIST = pd.read_csv(Path(METABOLITE.include).expanduser())["mz"].tolist()
        EXCLUDE_LIST = pd.read_csv(Path(METABOLITE.exclude).expanduser())["mz"].tolist()
    except:
        INCLUDE_LIST = None
        EXCLUDE_LIST = None
    
    return PARAMETERS, INCLUDE_LIST, EXCLUDE_LIST 
