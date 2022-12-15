from movici_api_client.cli.config import Config, Context, write_config
from movici_api_client.cli.exceptions import NoContextAvailable, NoSuchContext
from movici_api_client.cli.ui import format_dataclass

from .. import dependencies
from ..common import Controller
from ..utils import assert_context, confirm, echo, prompt
from ..decorators import command, argument, option


class ConfigController(Controller):
    name = "config"
    reverse = False

    @command
    def create(self):
        config = dependencies.get(Config)

        echo("Creating a new context, please give the following information")
        name = prompt("Name")
        url = prompt("Base URL (eg: https://example.org/)")
        context = Context(name, url)
        config.add_context(context)

        activate = confirm("Do you wish to activate this context?", default=True)
        if activate:
            config.activate_context(name)
        write_config(config)
        echo("Context succesfully created")

    @command
    @argument("context")
    def activate(self, context: str):
        config = dependencies.get(Config)
        if not len(config.contexts):
            raise NoContextAvailable()
        try:
            config.activate_context(context)
        except ValueError:
            raise NoSuchContext(context=context)

        write_config(config)

    @command
    @option("-a", "--all", is_flag=True)
    def show(self, all):
        config = dependencies.get(Config)
        if all:
            contexts = config.contexts
        else:
            contexts = [assert_context(config)]

        echo("\n\n".join(format_dataclass(c) for c in contexts))
