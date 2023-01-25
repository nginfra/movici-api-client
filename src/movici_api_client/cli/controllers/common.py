import pathlib
import typing as t

from movici_api_client.cli.data_dir import (
    DatasetsDirectory,
    MoviciDataDir,
    ScenariosDirectory,
    UpdatesDirectory,
    ViewsDirectory,
)
from movici_api_client.cli.exceptions import ClickException

from ..utils import echo


def resolve_data_directory(
    path: t.Union[str, pathlib.Path, None],
    purpose: t.Literal["datasets", "scenarios", "updates", "views", "project"],
):
    # TODO: if the directory is given, is inside a movici data dir and is a valid directory for
    # the its purpose, then return a specialized version of DataDir. This allows for example
    # uploading or downloading views or simulation results to a different scenario

    if data_dir := MoviciDataDir.resolve_from_subpath(path or pathlib.Path(".")):
        echo("Movici data directory detected")
        return data_dir

    if path is not None:
        path = path.resolve()
        return {
            "datasets": DatasetsDirectory,
            "scenarios": ScenariosDirectory,
            "updates": UpdatesDirectory,
            "views": ViewsDirectory,
            "project": MoviciDataDir,
        }[purpose](path)

    raise ClickException("Could not determine data directory")
