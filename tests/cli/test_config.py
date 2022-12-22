import json
import os
import pathlib

import pytest

from movici_api_client.cli.config import (
    CONFIG_LOCATION_ENV,
    Config,
    Context,
    get_config,
    get_config_path,
    read_config,
    write_config,
)
from movici_api_client.cli.exceptions import InvalidConfigFile


@pytest.fixture(autouse=True)
def clean_env():
    try:
        del os.environ[CONFIG_LOCATION_ENV]
    except KeyError:
        pass


@pytest.fixture
def config_file(tmp_path):
    file = tmp_path.joinpath(".movici.conf")
    file.write_text(
        json.dumps(
            {
                "version": 1,
                "current_context": "a",
                "contexts": [{"name": "a", "url": "https://example.com"}],
            }
        )
    )
    return file


def test_default_config_location():
    result = get_config_path()
    assert result.name == ".movici.conf"
    assert "~" not in str(result)


def test_alternative_config_path_default():
    assert str(get_config_path(default="path/conf.conf")) == "path/conf.conf"


def test_set_config_env(tmp_path):
    os.environ["MOVICI_CLI_CONFIG"] = str(tmp_path / ".movici.conf")
    assert get_config_path().is_relative_to(tmp_path)


def test_alternative_config_env(tmp_path):
    os.environ["MOVICI_CLI_CONFIG_ALT"] = str(tmp_path / ".movici.conf")
    assert get_config_path(env="MOVICI_CLI_CONFIG_ALT").is_relative_to(tmp_path)


def test_get_config(config_file):
    config = get_config(config_file)
    assert config.version == 1


def test_creates_default_config(tmp_path):
    file = tmp_path.joinpath(".conf")
    assert not file.is_file()
    get_config(file)

    assert json.loads(file.read_text()) == {"version": 1, "current_context": None, "contexts": []}


@pytest.mark.parametrize(
    "content, msg",
    [
        (None, "read error"),
        ("not json", "invalid json"),
        ("{}", "invalid values"),
        (
            json.dumps({"version": 1, "current_context": "a", "contexts": [{"name": "a"}]}),
            "invalid values",
        ),
    ],
)
def test_invalid_config(tmp_path: pathlib.Path, content, msg):
    file = tmp_path.joinpath("subdir/.conf")
    if content is not None:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(content)
    with pytest.raises(InvalidConfigFile) as e:
        get_config(file)
    assert e.value.msg == msg
    assert e.value.file == file


class TestConfig:
    @pytest.fixture
    def context(self):
        return

    @pytest.fixture
    def config(self):
        return Config(contexts=[Context(name="my-context", url="https://example.org")])

    @pytest.fixture
    def activated_config(self, config):
        config.activate_context(config.contexts[0].name)
        return config

    def test_default_current_context(self, config):
        assert config.current_context is None

    def test_activate_context(self, activated_config):
        assert activated_config.current_context.name == "my-context"

    def test_serializes_active_context(self, activated_config):
        assert activated_config.as_dict()["current_context"] == "my-context"

    def test_get_config(self, config):
        assert config.get_context("my-context").url == "https://example.org"

    def test_add_context(self, config):
        config.add_context(Context("new-context", "https://some.url"))
        assert len(config.contexts) == 2

    def test_activate_new_context(self, config):
        config.add_context(Context("new-context", "https://some.url"))
        config.activate_context("new-context")
        assert config.current_context.url == "https://some.url"

    def test_remove_context_by_object(self, config):
        context = config.contexts[0]
        config.remove_context(context)
        assert context not in config.contexts

    def test_remove_context_by_name(self, config):
        context = config.contexts[0]
        config.remove_context(context.name)
        assert context not in config.contexts

    def test_remove_active_context_deactivates(self, activated_config):
        context = activated_config.contexts[0]
        activated_config.remove_context(context)
        assert activated_config.current_context is None

    def test_remove_nonexisting_context(self, config):
        config.remove_context("invalid")
        assert len(config.contexts) == 1

    def test_cannot_activate_removed_context(self, config):
        context = config.contexts[0]
        config.remove_context(context)
        with pytest.raises(ValueError):
            config.activate_context(context.name)

    def test_config_as_dict(self, activated_config):
        activated_config.add_context(
            Context(
                "some-context",
                "https://some.url",
                project="some_project",
                username="some_user",
                auth_token="abcdef",
            )
        )
        assert activated_config.as_dict() == {
            "version": 1,
            "current_context": "my-context",
            "contexts": [
                {
                    "name": "my-context",
                    "url": "https://example.org",
                },
                {
                    "name": "some-context",
                    "url": "https://some.url",
                    "project": "some_project",
                    "username": "some_user",
                    "auth_token": "abcdef",
                },
            ],
        }

    def test_can_rename_current_config(self, activated_config):
        activated_config.current_context.name = "new-name"
        assert activated_config.as_dict()["current_context"] == "new-name"


def test_serialize_and_deserialize_config(tmp_path):
    file = tmp_path.joinpath(".conf")
    context = Context(
        name="a",
        url="https://example.org",
        project="project",
        username="username",
        auth_token="auth_token",
    )
    config = Config(version=1, current_context="a", contexts=[context])
    write_config(config, file)
    result = read_config(file)
    assert result == config


class TestContext:
    @pytest.fixture
    def context(self):
        return Context("foo", "https://example.org")

    def test_context_set_auth_boolean(self, context):
        context["auth"] = "false"
        assert context["auth"] is False

    def test_default_value(self, context):
        assert context["auth"] is True
