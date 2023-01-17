from unittest.mock import Mock

import pytest

from movici_api_client.api import requests
from movici_api_client.api.client import Client


@pytest.fixture
def client():
    return Client(base_url="", client=Mock())


def test_login(client):
    assert requests.Login("user", "password").generate_config(client) == {
        "method": "POST",
        "url": "/auth/v1/user/login/",
        "json": {"username": "user", "password": "password"},
    }


def test_check_auth_token(client):
    assert requests.CheckAuthToken("abc").generate_config(client) == {
        "method": "GET",
        "url": "/auth/v1/auth/",
        "headers": {"Authorization": "abc"},
    }


def test_check_auth_token_default(client):
    assert requests.CheckAuthToken().generate_config(client) == {
        "method": "GET",
        "url": "/auth/v1/auth/",
    }


def test_get_projects(client):
    assert requests.GetProjects().generate_config(client) == {
        "method": "GET",
        "url": "/data-engine/v4/projects/",
    }


def test_get_project(client):
    assert requests.GetSingleProject("0000-0000").generate_config(client) == {
        "method": "GET",
        "url": "/data-engine/v4/projects/0000-0000/",
    }


def test_create_project(client):
    assert requests.CreateProject(
        name="some_project", display_name="Some Project"
    ).generate_config(client) == {
        "method": "POST",
        "url": "/data-engine/v4/projects/",
        "json": {"name": "some_project", "display_name": "Some Project"},
    }


def test_update_project(client):
    assert requests.UpdateProject(uuid="0000-0000", display_name="Some Project").generate_config(
        client
    ) == {
        "method": "PUT",
        "url": "/data-engine/v4/projects/0000-0000/",
        "json": {"display_name": "Some Project"},
    }
