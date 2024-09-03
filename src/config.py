from configparser import ConfigParser
from typing import Any


def config(file_path: str = "database.ini", section: str = "postgresql") -> Any:
    parser = ConfigParser()
    parser.read(file_path)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception("Section {0} is not found in the {1} file.".format(section, file_path))
    return db


config()
