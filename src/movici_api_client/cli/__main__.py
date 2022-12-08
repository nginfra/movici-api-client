from movici_api_client.cli.bootstrap import cli_factory
from movici_api_client.cli.controllers.config import ConfigController
from movici_api_client.cli.main import activate_project, main, login
from movici_api_client.cli.controllers.projects import ProjectController

cli_factory(
        main=main,
        commands=[login, activate_project],
        controller_types=[
            ProjectController,
            ConfigController
        ]
    )()