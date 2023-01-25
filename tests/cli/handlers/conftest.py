def pytest_configure(config):
    config.addinivalue_line("markers", "no_valid_project_uuid")
