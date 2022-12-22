import json
from unittest.mock import Mock, call, patch

import pytest

import movici_api_client.cli.filetransfer
from movici_api_client.cli.filetransfer import (
    DatasetUploadStrategy,
    MultipleResourceUploader,
    ResourceUploader,
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


class TestResourceUploader:
    @pytest.fixture
    def upload_file(self, add_dataset):
        return add_dataset("dataset", {"type": "some_type"})

    @pytest.fixture
    def uploader(self, upload_file, strategy):
        project_uuid = "0000-0000"
        return ResourceUploader(upload_file, project_uuid, strategy)

    def test_upload_ensures_resources(self, uploader, strategy):
        uploader.upload(False, False, False)

        assert strategy.get_all.call_count == 1

    def test_creates_new_if_not_exists(self, uploader, strategy, upload_file):
        strategy.get_all.return_value = []
        uploader.upload(overwrite=False, create_new=True, inspect_file=False)
        assert strategy.create_new.call_args == call(
            uploader.parent_uuid, file=upload_file, name=upload_file.stem, inspect_file=False
        )

    def test_create_new_can_override_name(self, uploader, strategy, upload_file):
        strategy.get_all.return_value = []
        uploader.upload(overwrite=False, create_new=True, inspect_file=False, name="alternative")
        assert strategy.create_new.call_args == call(
            uploader.parent_uuid, file=upload_file, name="alternative", inspect_file=False
        )

    def test_doesnt_create_new_when_flag_not_set(self, uploader, strategy):
        strategy.get_all.return_value = []
        uploader.upload(overwrite=False, create_new=False, inspect_file=False)
        assert strategy.create_new.call_count == 0

    def test_checks_overwrite_required_on_existing(self, uploader, strategy):
        existing = {"name": "dataset"}
        strategy.get_all.return_value = [existing]
        uploader.upload(overwrite=True, create_new=False, inspect_file=False)
        assert strategy.require_overwrite_question.call_args == call(existing)

    def test_overwrites_on_existing(self, uploader, strategy, upload_file):
        existing = {"name": "dataset"}
        strategy.get_all.return_value = [existing]
        uploader.upload(overwrite=True, create_new=False, inspect_file=False)
        assert strategy.update_existing.call_args == call(existing, upload_file, False)

    @pytest.mark.parametrize(
        "require_overwrite_question, determine_overwrite, update_called",
        [
            (False, False, True),
            (True, False, False),
            (False, True, True),
            (True, True, True),
        ],
    )
    def test_overwrites_when_checks_pass(
        self, require_overwrite_question, determine_overwrite, update_called, uploader, strategy
    ):
        strategy.get_all.return_value = [{"name": "dataset"}]
        strategy.require_overwrite_question.return_value = require_overwrite_question

        uploader.upload(overwrite=determine_overwrite, create_new=False, inspect_file=False)

        assert strategy.update_existing.call_count == int(update_called)

    @pytest.mark.parametrize(
        "flag,confirm,expected",
        [
            (True, None, True),
            (False, None, False),
            (None, True, True),
            (None, False, False),
        ],
    )
    def test_maybe_questions(self, flag, confirm, expected, uploader):
        with patch.object(movici_api_client.cli.filetransfer, "confirm") as mock:
            mock.return_value = confirm
            assert uploader.determine_create_new(flag, "some_name") == expected
            assert uploader.determine_overwrite(flag, "some_name") == expected


class TestMultipleResourceUploader:
    @pytest.fixture
    def uploader(self, strategy, data_dir):
        return MultipleResourceUploader(data_dir, "0000-0000", strategy, uploader_cls=Mock())

    def test_calls_upload_multiple_times(self, add_dataset, uploader):
        add_dataset("dataset1")
        add_dataset("dataset2")
        uploader.upload(True, True, True)
        assert uploader.uploader_cls().upload.call_count == 2

    @pytest.mark.parametrize(
        "overwrite, create_new, inspect",
        [
            (True, True, True),
            (False, False, False),
        ],
    )
    def test_passes_flags(self, overwrite, create_new, inspect, add_dataset, uploader):
        add_dataset("dataset")
        uploader.upload(overwrite, create_new, inspect)
        assert uploader.uploader_cls().upload.call_args == call(
            overwrite=overwrite, create_new=create_new, inspect_file=inspect
        )


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
    def test_infer_dataset_type(self, filename, data, inspect, expected_value, add_dataset):
        strategy = DatasetUploadStrategy()
        strategy.all_dataset_types = ["a", "b"]
        file = add_dataset(filename, data)
        with patch.object(movici_api_client.cli.filetransfer, "prompt_choices") as prompt:
            prompt.return_value = self.prompt_sentinel
            assert strategy.infer_dataset_type(file, inspect) == expected_value

    @pytest.mark.parametrize(
        "extensions,files,result",
        [
            (None, ["a.json", "b.csv", "c.png"], ["a.json", "b.csv", "c.png"]),
            ((".json", ".csv"), ["a.json", "b.csv", "c.png"], ["a.json", "b.csv"]),
        ],
    )
    def test_iter_dataset_files(self, extensions, files, result, add_dataset, data_dir):
        strategy = DatasetUploadStrategy()
        strategy.extensions = extensions

        for file in files:
            add_dataset(file)
        assert set(f.name for f in strategy.iter_files(data_dir)) == set(result)
