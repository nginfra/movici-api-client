import functools
import json
from unittest.mock import Mock, call, patch

import pytest

import movici_api_client.cli.filetransfer.common
import movici_api_client.cli.filetransfer.upload
from movici_api_client.api.client import AsyncClient
from movici_api_client.cli.filetransfer import (
    DatasetUploadStrategy,
    UploadResource,
    UploadStrategy,
)


@pytest.fixture
def data_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("data")


@pytest.fixture
def add_dataset(data_dir):
    def _add_dataset(name, contents=None):
        if contents is None:
            contents = ""
        if isinstance(contents, dict):
            contents = json.dumps(contents)
        file = data_dir.joinpath(name)
        file.write_text(contents)
        return file

    return _add_dataset


@pytest.fixture
def strategy(data_dir):
    mock = Mock(UploadStrategy)
    mock.resource_type = "resource"
    mock.get_all.return_value = []
    mock.iter_files.side_effect = lambda d: list(d.glob("*"))
    return mock


class TestUploadResource:
    @pytest.fixture
    def upload_file(self, add_dataset):
        return add_dataset("dataset", {"type": "some_type"})

    @pytest.fixture
    def make_task(self, upload_file, strategy):
        project_uuid = "0000-0000"
        return functools.partial(
            UploadResource,
            parent_uuid=project_uuid,
            client=AsyncClient(""),
            file=upload_file,
            strategy=strategy,
        )

    @pytest.mark.asyncio
    async def test_upload_ensures_resources(self, make_task, strategy):
        task = make_task(overwrite=False, create_new=True, inspect_file=False)
        await task.run()
        assert strategy.get_all.await_count == 1

    @pytest.mark.asyncio
    async def test_creates_new_if_not_exists(self, make_task, strategy, upload_file):
        task = make_task(overwrite=False, create_new=True, inspect_file=False)
        await task.run()

        assert strategy.create_new.await_args == call(
            task.parent_uuid, file=upload_file, name=upload_file.stem, inspect_file=False
        )

    @pytest.mark.asyncio
    async def test_create_new_can_override_name(self, make_task, strategy, upload_file):
        strategy.get_all.return_value = []
        task = make_task(overwrite=False, create_new=True, inspect_file=False, name="alternative")
        await task.run()
        assert strategy.create_new.await_args == call(
            task.parent_uuid, file=upload_file, name="alternative", inspect_file=False
        )

    @pytest.mark.asyncio
    async def test_doesnt_create_new_when_flag_not_set(self, make_task, strategy):
        strategy.get_all.return_value = []
        task = make_task(overwrite=False, create_new=False, inspect_file=False)
        await task.run()
        assert strategy.create_new.await_count == 0

    @pytest.mark.asyncio
    async def test_checks_overwrite_required_on_existing(self, make_task, strategy):
        existing = {"name": "dataset"}
        strategy.get_all.return_value = [existing]
        task = make_task(overwrite=True, create_new=False, inspect_file=False)
        await task.run()
        assert strategy.require_overwrite_question.call_args == call(existing)

    @pytest.mark.asyncio
    async def test_overwrites_on_existing(self, make_task, strategy, upload_file):
        existing = {"name": "dataset"}
        strategy.get_all.return_value = [existing]
        task = make_task(overwrite=True, create_new=False, inspect_file=False)
        await task.run()
        assert strategy.update_existing.await_args == call(existing, upload_file, False)

    @pytest.mark.parametrize(
        "require_overwrite_question, determine_overwrite, update_called",
        [
            (False, False, True),
            (True, False, False),
            (False, True, True),
            (True, True, True),
        ],
    )
    @pytest.mark.asyncio
    async def test_overwrites_when_checks_pass(
        self, require_overwrite_question, determine_overwrite, update_called, make_task, strategy
    ):
        strategy.get_all.return_value = [{"name": "dataset"}]
        strategy.require_overwrite_question.return_value = require_overwrite_question

        task = make_task(overwrite=determine_overwrite, create_new=False, inspect_file=False)
        await task.run()

        assert strategy.update_existing.await_count == int(update_called)

    @pytest.mark.parametrize(
        "flag,confirm,expected",
        [
            (True, None, True),
            (False, None, False),
            (None, True, True),
            (None, False, False),
        ],
    )
    @pytest.mark.asyncio
    async def test_maybe_questions(self, flag, confirm, expected, make_task):
        task = make_task(overwrite=False, create_new=False, inspect_file=False)
        with patch.object(movici_api_client.cli.filetransfer.common, "confirm") as mock:
            mock.return_value = confirm
            assert task.determine_create_new(flag, "some_name") == expected
            assert task.determine_overwrite(flag, "some_name") == expected


class TestDatasetUploadStrategy:
    prompt_sentinel = object()

    @pytest.mark.parametrize(
        "filename, data, inspect, expected_value",
        [
            ("file.csv", None, False, "parameters"),
            ("file.nc", None, False, "flooding_tape"),
            ("file.tiff", None, False, "height_map"),
            ("file.json", None, False, prompt_sentinel),
            ("file.json", {}, True, prompt_sentinel),
            ("file.json", {"type": "some_type"}, True, "some_type"),
            ("file.other", None, True, prompt_sentinel),
            ("file.other", None, False, prompt_sentinel),
        ],
    )
    @pytest.mark.asyncio
    async def test_infer_dataset_type(self, filename, data, inspect, expected_value, add_dataset):
        strategy = DatasetUploadStrategy(None)
        strategy.all_dataset_types = ["a", "b"]
        file = add_dataset(filename, data)
        with patch.object(
            movici_api_client.cli.filetransfer.upload, "prompt_choices_async"
        ) as prompt:
            prompt.return_value = self.prompt_sentinel
            assert await strategy.infer_dataset_type(file, inspect) == expected_value

    @pytest.mark.parametrize(
        "extensions,files,result",
        [
            (None, ["a.json", "b.csv", "c.png"], ["a.json", "b.csv", "c.png"]),
            ((".json", ".csv"), ["a.json", "b.csv", "c.png"], ["a.json", "b.csv"]),
        ],
    )
    def test_iter_dataset_files(self, extensions, files, result, add_dataset, data_dir):
        strategy = DatasetUploadStrategy(None)
        strategy.extensions = extensions

        for file in files:
            add_dataset(file)
        assert set(f.name for f in strategy.iter_files(data_dir)) == set(result)
