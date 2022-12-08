import pytest

from movici_api_client.cli.bootstrap import cli_factory
from movici_api_client.cli.controllers.config import ConfigController
from movici_api_client.cli.controllers.projects import ProjectController
from movici_api_client.cli.main import login, main
import click.testing


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
    result = runner.invoke(cli, ["config", "show"])
    assert result.exit_code == 0
