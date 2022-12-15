from movici_api_client.cli.bootstrap import cli_factory
from movici_api_client.cli.controllers.config import ConfigController
from movici_api_client.cli.controllers.dataset_types import DatasetTypeController
from movici_api_client.cli.controllers.datasets import DatasetCrontroller
from movici_api_client.cli.controllers.projects import ProjectController
from movici_api_client.cli.main import activate_project, login, main

cli_factory(
    main=main,
    commands=[login, activate_project],
    controller_types=[
        ProjectController,
        DatasetCrontroller,
        ConfigController,
        DatasetTypeController,
    ],
)()
