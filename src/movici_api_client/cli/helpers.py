import json
import pathlib

from .exceptions import InvalidFile


def read_json_file(file: pathlib.Path):
    if not file.is_file():
        raise InvalidFile("not a file")
    try:
        return json.loads(file.read_text())
    except IOError:
        raise InvalidFile("read error", file)
    except json.JSONDecodeError:
        raise InvalidFile("invalid json", file)
