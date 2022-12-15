import contextlib
from movici_api_client.api import Client
from movici_api_client.api.requests import AddDatasetData, ModifiyDatasetData
import pathlib
from tqdm.auto import tqdm
from tqdm.utils import CallbackIOWrapper
from . import dependencies


@contextlib.contextmanager
def monitored_file(file: pathlib.Path):
    with (
        open(file, "rb") as fobj,
        tqdm(
            total=file.stat().st_size, unit="B", unit_scale=True, unit_divisor=1024, desc=file.name
        ) as t,
    ):
        yield CallbackIOWrapper(t.update, fobj, "read")
        t.reset()


def upload_new_dataset(uuid, file: pathlib.Path):
    client = dependencies.get(Client)
    with monitored_file(file) as fobj:
        return client.request(AddDatasetData(uuid, fobj))


def upload_existing_dataset(uuid, file):
    client = dependencies.get(Client)
    with monitored_file(file) as fobj:
        return client.request(ModifiyDatasetData(uuid, fobj))
