import click.testing
import pytest

from movici_api_client.cli.bootstrap import cli_factory
from movici_api_client.cli.config import read_config
from movici_api_client.cli.controllers.config import ConfigController
from movici_api_client.cli.controllers.projects import ProjectController
from movici_api_client.cli.main import login, main


@pytest.fixture
def cli():
    return cli_factory(
        main=main,
        commands=[login],
        controller_types=[
            ProjectController,
            ConfigController,
        ],
    )


def test_integration(cli):
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ["config", "show"], catch_exceptions=False)
    assert result.exit_code == 0


def test_login_saves_context(cli, client, config_path):
    client.set_response({"session": "some_auth_token"})
    runner = click.testing.CliRunner()
    runner.invoke(cli, ["login"], input="user\npw\n", catch_exceptions=False)
    config = read_config(config_path)
    assert config.current_context.auth_token == "some_auth_token"
    assert config.current_context.username == "user"
