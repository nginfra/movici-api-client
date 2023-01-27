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
from movici_api_client.cli.events.dataset import (
    CreateDataset,
    DeleteDataset,
    GetAllDatasets,
    GetSingleDataset,
    UpdateDataset,
)
from movici_api_client.cli.events.project import (
    CreateProject,
    DeleteProject,
    DownloadProject,
    EditProject,
    GetAllProjects,
    GetSingleProject,
    UploadProject,
)
from movici_api_client.cli.exceptions import Conflict, InvalidUsage, NotFound
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
    assert data_dir.datasets().get_file_path("my_dataset.json")


class TestLocalUpdateDatasetHandler:
    @pytest.fixture
    def json_dataset(self, data_dir):
        data = {
            "name": "my_dataset",
            "display_name": "Display Name",
            "type": "some_type",
            "data": {"some": "data"},
        }
        file = data_dir.datasets().get_file_path(data["name"] + ".json")
        file.write_text(json.dumps(data))
        return data

    @pytest.fixture
    def binary_dataset(self, data_dir):
        name = "some_binary_dataset"
        data_dir.datasets().get_file_path(name + ".nc").write_bytes(b"123")
        return name

    @pytest.mark.asyncio
    async def test_update_json_dataset(self, json_dataset, data_dir: MoviciDataDir, mediator):
        new_file = data_dir.datasets().get_file_path("new_name.json")
        result = await mediator.send(
            UpdateDataset(
                name_or_uuid="my_dataset",
                name="new_name",
                display_name="New Display Name",
                type="new_type",
            )
        )
        assert result == "Dataset succesfully updated"
        assert not data_dir.datasets().get_file_path(json_dataset["name"] + ".json").exists()
        assert json.loads(new_file.read_text()) == {
            "name": "new_name",
            "display_name": "New Display Name",
            "type": "new_type",
            "data": {"some": "data"},
        }

    @pytest.mark.asyncio
    async def test_update_binary_dataset(self, binary_dataset, data_dir: MoviciDataDir, mediator):
        new_file = data_dir.datasets().get_file_path("new_name.nc")
        result = await mediator.send(
            UpdateDataset(
                name_or_uuid=binary_dataset,
                name="new_name",
            )
        )
        assert result == "Dataset succesfully updated"
        assert not data_dir.datasets().get_file_path(binary_dataset + ".json").exists()
        assert new_file.read_bytes() == b"123"

    @pytest.mark.asyncio
    async def test_not_exists(self, mediator):
        with pytest.raises(NotFound):
            await mediator.send(UpdateDataset("invalid", name="new_name"))

    @pytest.mark.asyncio
    async def test_duplicate_target(self, mediator, binary_dataset, json_dataset):
        with pytest.raises(Conflict):
            await mediator.send(UpdateDataset(json_dataset["name"], name=binary_dataset))


@pytest.mark.asyncio
async def test_local_delete_dataset_handler(mediator, data_dir, patch_confirm, default_dataset):
    assert data_dir.datasets().get_file_path_if_exists(default_dataset["name"])
    await mediator.send(DeleteDataset(default_dataset["name"]))

    assert patch_confirm.called
    assert not data_dir.datasets().get_file_path_if_exists(default_dataset["name"])
