from movici_api_client.cli.common import (
    get_options,
    set_options,
    has_options,
    remove_options,
)
import pytest


@pytest.fixture
def func():
    def func():
        pass

    return func


@pytest.mark.parametrize("options", [{}, {"some": "option"}])
def test_set_options(func, options):
    assert not has_options(func, "key")
    set_options(func, "key", options)
    assert has_options(func, "key")


def test_set_options_empty(func):
    assert not has_options(func, "key")
    set_options(func, "key", {})
    assert has_options(func, "key")


def test_get_options(func):
    set_options(func, "key", {"some": "option"})
    assert get_options(func, "key") == {"some": "option"}


def test_multiple_options(func):
    set_options(func, "key", {})
    assert not has_options(func, "other")
    set_options(func, "other", {"other": "options"})
    assert has_options(func, "other")
    assert has_options(func, "key")
    assert get_options(func, "key") != get_options(func, "other")


def test_merges_options(func):
    set_options(func, "key", {"some": "option"})
    set_options(func, "key", {"other": "option"})
    assert get_options(func, "key") == {"some": "option", "other": "option"}


def test_overwrite_existing_options(func):
    set_options(func, "key", {"some": "option"})
    set_options(func, "key", {"some": "other"})
    assert get_options(func, "key") == {"some": "other"}


def test_overwrite_options(func):
    set_options(func, "key", {"some": "option"})
    set_options(func, "key", {"other": "option"})


def test_remove_options(func):
    set_options(func, "key", {})
    remove_options(func, "key")
    assert not has_options(func, "key")


def test_remove_options_if_not_exists(func):
    remove_options(func, "key")
    assert not has_options(func, "key")
