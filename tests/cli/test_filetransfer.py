import json
from unittest.mock import patch
import pytest
import movici_api_client.cli.filetransfer
from movici_api_client.cli.filetransfer import MultipleDatasetUploader


class TestMultipleDatasetUploader:
    @pytest.fixture
    def data_dir(self, tmp_path_factory):
        return tmp_path_factory.mktemp("data")

    @pytest.fixture
    def add_dataset(self, data_dir):
        def _add_dataset(file, contents=None):
            if contents is None:
                contents = ""
            if isinstance(contents, dict):
                contents = json.dumps(contents)
            file = data_dir.joinpath(file)
            file.write_text(contents)
            return file

        return _add_dataset

    @pytest.fixture
    def uploader(self, client, data_dir):
        project_uuid = "0000-0000"
        return MultipleDatasetUploader(data_dir, project_uuid, client)

    @pytest.mark.parametrize(
        "extensions,files,result",
        [
            (None, ["a.json", "b.csv", "c.png"], ["a.json", "b.csv", "c.png"]),
            ((".json", ".csv"), ["a.json", "b.csv", "c.png"], ["a.json", "b.csv"]),
        ],
    )
    def test_iter_dataset_files(self, extensions, files, result, uploader, add_dataset):
        for file in files:
            add_dataset(file)
        uploader.extensions = extensions
        assert set(f.name for f in uploader.iter_dataset_files()) == set(result)

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
            assert uploader.maybe_create_new_dataset(flag, "some_name") == expected
            assert uploader.maybe_overwrite_data(flag, "some_name") == expected

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
    def test_infer_dataset_type(
        self, filename, data, inspect, expected_value, uploader, add_dataset
    ):
        uploader.all_dataset_types = ["a", "b"]
        file = add_dataset(filename, data)
        with patch.object(movici_api_client.cli.filetransfer, "prompt_choices") as prompt:
            prompt.return_value = self.prompt_sentinel
            assert uploader.infer_dataset_type(file, inspect) == expected_value
