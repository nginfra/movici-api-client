import pytest

from movici_api_client.cli.data_dir import (
    MOVICI_DATADIR_SENTINEL,
    DatasetsDirectory,
    MoviciDataDir,
    ScenariosDirectory,
    SimpleDataDirectory,
    UpdatesDirectory,
    ViewsDirectory,
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


@pytest.mark.parametrize("directory", ["views", "updates"])
def test_ensure_subdirectory(movici_data_dir: MoviciDataDir, directory):
    subdir = getattr(movici_data_dir, directory)("some_scenario")
    target = subdir.path
    assert not target.is_dir()
    path = subdir.ensure_directory()
    assert path == target
    assert path.is_dir()


def test_resolve_from_subpath(movici_data_dir: MoviciDataDir):
    subpath = movici_data_dir.views("some_scenario").ensure_directory()
    assert MoviciDataDir.resolve_from_subpath(subpath) == movici_data_dir


def test_subpath_resolution_failure(tmp_path):
    assert MoviciDataDir.resolve_from_subpath(tmp_path) is None


def test_ensure_directory_fails_when_not_a_directory(tmp_path):
    path = tmp_path.joinpath("not_a_dir")
    path.touch()
    with pytest.raises(InvalidDirectory):
        MoviciDataDir(path).ensure_directory()


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
    assert set(f.name for f in data_dir.iter_files()) == set(result)


@pytest.mark.parametrize(
    "subdir, cls",
    [
        ("datasets", DatasetsDirectory),
        ("scenarios", ScenariosDirectory),
        ("views", ViewsDirectory),
        ("updates", UpdatesDirectory),
    ],
)
def test_sub_datadir_returns_relevant_instance(movici_data_dir, subdir, cls):
    assert isinstance(getattr(movici_data_dir, subdir)(), cls)


@pytest.mark.parametrize(
    "name, filenames, exists",
    [
        ("some_file", ["some_file.json"], True),
        ("some_file", ["some_other_file.json"], False),
        ("some_file", ["some_other_file.json", "some_file.nc"], True),
    ],
)
def test_get_file_path_if_exists(movici_data_dir: MoviciDataDir, name, filenames, exists):
    data_dir = movici_data_dir.datasets()
    for file in filenames:
        data_dir.path.joinpath(file).touch()

    assert bool(data_dir.get_file_path_if_exists(name)) == exists


def test_get_file_path(movici_data_dir: MoviciDataDir):
    data_dir = movici_data_dir.datasets()
    assert data_dir.get_file_path("somefile.json") == data_dir.path.joinpath("somefile.json")
