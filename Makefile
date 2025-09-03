unittest:
	poetry run pytest -v tests/

test-cov:
	poetry run pytest --cov=movici_api_client --cov-report=term-missing tests/

flake8:
	poetry run flake8 src/ tests/

black-check:
	poetry run black --check .

isort:
	poetry run isort .
	
isort-check:
	poetry run isort -c .

lint: flake8 black-check isort-check
	
test-all: lint test-cov

level=patch
export level

bump-version:
	bumpversion  --config-file .bumpversion.app $(level)
	@NEW_VERSION=$$(tail -1 VERSION);\
	echo New version: $$NEW_VERSION
