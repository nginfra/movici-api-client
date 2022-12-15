from movici_api_client.api import requests


def test_login():
    assert requests.Login("user", "password").make_request() == {
        "method": "POST",
        "url": "/auth/v1/user/login/",
        "json": {"username": "user", "password": "password"},
    }


def test_check_auth_token():
    assert requests.CheckAuthToken("abc").make_request() == {
        "method": "GET",
        "url": "/auth/v1/auth/",
        "headers": {"Authorization": "abc"},
    }


def test_check_auth_token_default():
    assert requests.CheckAuthToken().make_request() == {
        "method": "GET",
        "url": "/auth/v1/auth/",
    }


def test_get_projects():
    assert requests.GetProjects().make_request() == {
        "method": "GET",
        "url": "/data-engine/v4/projects/",
    }


def test_get_project():
    assert requests.GetSingleProject("0000-0000").make_request() == {
        "method": "GET",
        "url": "/data-engine/v4/projects/0000-0000/",
    }


def test_create_project():
    assert requests.CreateProject(
        name="some_project", display_name="Some Project"
    ).make_request() == {
        "method": "POST",
        "url": "/data-engine/v4/projects/",
        "json": {"name": "some_project", "display_name": "Some Project"},
    }


def test_update_project():
    assert requests.UpdateProject(
        uuid="0000-0000", display_name="Some Project"
    ).make_request() == {
        "method": "PUT",
        "url": "/data-engine/v4/projects/0000-0000/",
        "json": {"display_name": "Some Project"},
    }
