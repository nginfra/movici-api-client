import click
import click.testing
import pytest

from movici_api_client.cli.bootstrap import create_click_command, register_controller
from movici_api_client.cli.common import Controller
from movici_api_client.cli.decorators import argument, command


class MyController(Controller):
    name: str = "single"

    @command
    def get(self):
        click.echo("get")

    @command(name="multiple")
    def list(self):
        click.echo("list")


def test_register_controller():
    group = click.Group("main")
    register_controller(group, MyController())
    assert "single" in group.commands["get"].commands
    assert "multiple" in group.commands["list"].commands


def test_register_controller_with_explicit_group():
    class MyController(Controller):
        name: str = "single"

        @command
        def get(self):
            click.echo("get")

        @command(name="multiple", group="get")
        def get_multiple(self):
            click.echo("list")

    group = click.Group("main")
    register_controller(group, MyController())
    assert "single" in group.commands["get"].commands
    assert "multiple" in group.commands["get"].commands


@pytest.mark.parametrize(
    "args, output",
    [
        (["get", "single"], "get"),
        (["list", "multiple"], "list"),
    ],
)
def test_controller_integration(args, output):
    cli = click.Group("main")
    register_controller(cli, MyController())
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, args)
    assert result.exit_code == 0
    assert output in result.output


def test_command_with_arguments():
    @command
    @argument("arg")
    def func(arg):
        click.echo(arg)

    cli = create_click_command(func)
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ["foo"])
    assert result.exit_code == 0
    assert "foo" in result.output
