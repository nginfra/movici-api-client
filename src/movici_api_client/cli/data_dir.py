from __future__ import annotations

import pathlib
import re
import typing as t

from movici_api_client.cli.exceptions import InvalidDirectory

MOVICI_DATADIR_SENTINEL = ".movici_data"


class DataDir:
    def __init__(self, path: pathlib.Path) -> None:
        self.path = path

    def datasets(self) -> DatasetsDirectory:
        raise NotImplementedError

    def scenarios(self) -> ScenariosDirectory:
        raise NotImplementedError

    def updates(self, scenario="") -> UpdatesDirectory:
        raise NotImplementedError

    def views(self, scenario="") -> ViewsDirectory:
        raise NotImplementedError

    def ensure_directory(self):
        path = self.path
        if not path.exists():
            path.mkdir(exist_ok=True, parents=True)
        if not path.is_dir():
            raise InvalidDirectory("not a directory", path)
        return path


class MoviciDataDir(DataDir):
    def __init__(self, path: pathlib.Path) -> None:
        self.path = path

    def datasets(self):
        return DatasetsDirectory(self.path.joinpath("init_data"))

    def scenarios(self):
        return ScenariosDirectory(self.path.joinpath("scenarios"))

    def views(self, scenario=""):
        return ViewsDirectory(self.path.joinpath("views").joinpath(scenario))

    def updates(self, scenario=""):
        return UpdatesDirectory(self.scenarios().path.joinpath(scenario))

    @property
    def _sentinel(self):
        return self.path.joinpath(MOVICI_DATADIR_SENTINEL)

    def __eq__(self, other):
        if not isinstance(other, MoviciDataDir):
            return NotImplemented
        return self.path == other.path

    @classmethod
    def resolve_from_subpath(cls, path: t.Union[str, pathlib.Path]) -> t.Optional[pathlib.Path]:
        path = pathlib.Path(path).resolve()

        for _ in range(100):
            if path.joinpath(MOVICI_DATADIR_SENTINEL).is_file():
                return cls(path)
            if str(path) == path.anchor:
                break
            path = path.parent

        return None

    def initialize(self):
        if not self.path.exists():
            self.path.mkdir(parents=True, exist_ok=True)
        self.create_tree()

    def create_tree(self):
        if not self.path.is_dir():
            raise InvalidDirectory("not a directory", self.path)
        self._sentinel.touch()
        self.datasets().ensure_directory()
        self.scenarios().ensure_directory()
        self.views().ensure_directory()


class SimpleDataDirectory(DataDir):
    extensions: t.Optional[t.Collection[str]] = None

    def iter_files(self):
        if not self.path.is_dir():
            return
        for candidate in self.path.iterdir():
            if not candidate.is_file():
                continue
            if self.extensions is None or candidate.suffix in self.extensions:
                yield candidate

    def get_file_path_if_exists(self, name, suffix=None):
        extensions = self.extensions if suffix is None else [suffix]
        for ext in extensions:
            path = self.path.joinpath(name).with_suffix(ext)
            if path.is_file():
                return path

    def get_file_path(self, name):
        return self.path.joinpath(name)


class DatasetsDirectory(SimpleDataDirectory):
    extensions = {".json", ".msgpack", ".csv", ".nc", ".tiff", ".tif", ".geotif", ".geotif"}

    def updates(self):
        return self


class ScenariosDirectory(SimpleDataDirectory):
    extensions = {".json"}

    def scenarios(self):
        return self

    def updates(self, scenario="") -> UpdatesDirectory:
        return UpdatesDirectory(self.path.joinpath(scenario))


class UpdatesDirectory(SimpleDataDirectory):
    extensions = {".json"}

    def updates(self, scenario=""):
        return self

    def iter_files(self, scenario: str = None):
        pattern = re.compile(r"t(?P<timestamp>\d+)_(?P<iteration>\d+)_(?P<dataset>\w+)")
        for file in super().iter_files():
            if pattern.match(file.stem):
                yield file


class ViewsDirectory(SimpleDataDirectory):
    extensions = {".json"}

    def views(self, scenario=""):
        return self
