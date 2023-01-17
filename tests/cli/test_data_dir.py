import pytest

from movici_api_client.cli.data_dir import (
    MOVICI_DATADIR_SENTINEL,
    MoviciDataDir,
    SimpleDataDirectory,
)
from movici_api_client.cli.exceptions import InvalidDirectory


@pytest.fixture
def movici_data_dir(tmp_path):
    rv = MoviciDataDir(tmp_path)
    rv.initialize()
    return rv


def test_initialize_creates_directories(movici_data_dir):
    for subdir in ("init_data", "scenarios", "views"):
        assert movici_data_dir.path.joinpath(subdir).is_dir()


def test_create_tree_creates_sentinel_file(movici_data_dir):
    assert movici_data_dir.path.joinpath(MOVICI_DATADIR_SENTINEL).is_file()


def test_ensure_views_dir(movici_data_dir):
    target = movici_data_dir.views.joinpath("some_scenario")
    assert not target.is_dir()
    path = movici_data_dir.ensure_views_dir("some_scenario")
    assert path == target
    assert path.is_dir()


def test_ensure_simulation_dir(movici_data_dir):
    target = movici_data_dir.scenarios.joinpath("some_scenario")
    assert not target.is_dir()
    path = movici_data_dir.ensure_simulation_dir("some_scenario")
    assert path == target
    assert path.is_dir()


def test_resolve_from_subpath(movici_data_dir):
    subpath = movici_data_dir.ensure_views_dir("some_scenario")
    assert MoviciDataDir.resolve_from_subpath(subpath) == movici_data_dir


def test_subpath_resolution_failure(tmp_path):
    assert MoviciDataDir.resolve_from_subpath(tmp_path) is None


def test_ensure_directory_fails_when_not_a_directory(tmp_path):
    path = tmp_path.joinpath("not_a_dir")
    path.touch()
    with pytest.raises(InvalidDirectory):
        MoviciDataDir._ensure_directory(path)


@pytest.mark.parametrize(
    "extensions,files,result",
    [
        (None, ["a.json", "b.csv", "c.png"], ["a.json", "b.csv", "c.png"]),
        ((".json", ".csv"), ["a.json", "b.csv", "c.png"], ["a.json", "b.csv"]),
    ],
)
def test_iter_files(extensions, files, result, tmp_path_factory):
    path = tmp_path_factory.mktemp("iter_files")
    data_dir = SimpleDataDirectory(path)
    data_dir.extensions = extensions

    for file in files:
        path.joinpath(file).touch()
    assert set(f.name for f in data_dir._iter_files()) == set(result)
