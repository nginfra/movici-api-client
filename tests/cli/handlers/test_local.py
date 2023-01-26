import json
import pathlib
from contextvars import Context
from unittest.mock import patch
from uuid import UUID

import pytest

import movici_api_client.cli.handlers.local
from movici_api_client.cli.common import CLIParameters
from movici_api_client.cli.cqrs import Mediator
from movici_api_client.cli.data_dir import MoviciDataDir
from movici_api_client.cli.events.dataset import CreateDataset, GetAllDatasets, GetSingleDataset
from movici_api_client.cli.events.project import (
    CreateProject,
    DeleteProject,
    DownloadProject,
    EditProject,
    GetAllProjects,
    GetSingleProject,
    UploadProject,
)
from movici_api_client.cli.exceptions import InvalidUsage
from movici_api_client.cli.handlers import LOCAL_HANDLERS
from movici_api_client.cli.handlers.local import LocalDatasetsHandler


def uuid(int: int):
    return str(UUID(int=int))


@pytest.fixture
def data_dir(tmp_path):
    rv = MoviciDataDir(tmp_path)
    rv.initialize()
    return rv


@pytest.fixture
def add_dataset(data_dir: MoviciDataDir):
    def _add(dataset: dict, name=None):
        if name is None:
            name = dataset["name"]
        data_dir.datasets().path.joinpath(name).with_suffix(".json").write_text(
            json.dumps(dataset)
        )

    return _add


@pytest.fixture
def data_dir_with_data(add_dataset, default_dataset):
    add_dataset(default_dataset)


@pytest.fixture
def current_context(data_dir):
    with patch.object(movici_api_client.cli.handlers.local, "assert_current_context") as mock:
        mock.return_value = Context("local_dummy", location=str(data_dir.path))
        yield mock


@pytest.fixture(autouse=True)
def patch_confirm():
    with patch.object(movici_api_client.cli.handlers.local, "confirm") as mock:
        yield mock


@pytest.fixture
def cli_params():
    return CLIParameters()


@pytest.fixture(autouse=True)
def setup_gimme(gimme_repo, data_dir, cli_params):
    gimme_repo.add(data_dir)
    gimme_repo.add(cli_params)


@pytest.fixture
def mediator():
    return Mediator(LOCAL_HANDLERS)


@pytest.fixture
def default_dataset(data_dir):
    data = {
        "uuid": uuid(1),
        "name": "some_dataset",
        "type": "antennas_point_set",
        "display_name": "Some Dataset",
        "format": "entity_based",
    }
    data_dir.datasets().path.joinpath("some_dataset.json").write_text(json.dumps(data))
    return data


@pytest.mark.parametrize(
    "event",
    [
        GetAllProjects(),
        GetSingleProject(""),
        CreateProject("", ""),
        UploadProject(None),
        DeleteProject(""),
        UploadProject(None),
        DownloadProject(None),
        EditProject(""),
    ],
)
@pytest.mark.asyncio
async def test_local_project_handlers_raise(event, mediator):
    with pytest.raises(InvalidUsage):
        await mediator.send(event)


class TestLocalDatasetHandler:
    def _get_dataset_meta_testcases():
        def pick(source_dict: dict, keys):
            return {key: source_dict[key] for key in set(keys) & source_dict.keys()}

        base_dataset = {
            "name": "some_dataset",
            "type": "antennas_point_set",
            "display_name": "Some Dataset",
            "uuid": uuid(1),
            "format": "entity_based",
        }
        yield pick(base_dataset, ("name",)), None, False, pick(base_dataset, ("name",))
        yield pick(base_dataset, ("name",)), "some_other_dataset.json", False, {
            "name": "some_other_dataset"
        }
        yield base_dataset, "some_other_dataset.json", True, {
            **base_dataset,
            "name": "some_other_dataset",
            "has_data": False,
        }
        yield {**base_dataset, "data": {}}, None, True, {
            **base_dataset,
            "has_data": True,
        }

    @pytest.mark.parametrize("dataset, filename, inspect, expected", _get_dataset_meta_testcases())
    def test_get_dataset_meta(self, tmp_path, dataset, filename, inspect, expected):
        filename = filename or f"{dataset['name']}.json"
        file = tmp_path / filename
        raw_data = json.dumps(dataset).encode() if isinstance(dataset, dict) else dataset
        file.write_bytes(raw_data)
        handler = LocalDatasetsHandler(params=CLIParameters(inspect=inspect), directory=None)
        assert handler.get_dataset_meta(file) == expected

    @pytest.mark.parametrize(
        "file, dataset_type, expected_type, expected_format",
        [
            ("file.json", "antenna_point_set", "antenna_point_set", "entity_based"),
            ("file.json", None, None, None),
            ("file.nc", None, "flooding_tape", "binary"),
            ("file.tif", None, "height_map", "binary"),
            ("file.geotiff", None, "height_map", "binary"),
            ("file.json", "tabular", "tabular", "unstructured"),
        ],
    )
    def test_get_type_and_format(self, file, dataset_type, expected_type, expected_format):
        result = LocalDatasetsHandler.get_type_and_format(pathlib.Path(file), dataset_type)
        assert result == (expected_type, expected_format)


@pytest.mark.asyncio
@pytest.mark.usefixtures("data_dir_with_data")
async def test_local_get_all_datasets_handler(default_dataset, mediator):
    assert (await mediator.send(GetAllDatasets())) == [{"name": default_dataset["name"]}]


@pytest.mark.asyncio
@pytest.mark.usefixtures("data_dir_with_data")
async def test_local_get_single_dataset_handler(default_dataset, mediator):
    result = await mediator.send(GetSingleDataset(default_dataset["name"]))
    assert result == {"name": default_dataset["name"]}


@pytest.mark.asyncio
async def test_local_create_dataset_handler(data_dir, mediator):
    result = await mediator.send(
        CreateDataset(name="my_dataset", display_name="My Dataset", type="my_type")
    )
    assert result == "Dataset succesfully created"
    assert data_dir.datasets().path.joinpath()
