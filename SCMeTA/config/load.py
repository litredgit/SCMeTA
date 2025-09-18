from configparser import ConfigParser
from pathlib import Path

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
