unittest:
	poetry run pytest -v tests/

ruff:
	poetry run ruff check .

black-check:
	poetry run black --check .

isort:
	poetry run isort .
	
isort-check:
	poetry run isort -c .

lint: ruff black-check isort-check
	
test-all: lint unittest

level=patch
export level

bump-version:
	bumpversion  --config-file .bumpversion.app $(level)
	@NEW_VERSION=$$(tail -1 VERSION);\
	echo New version: $$NEW_VERSION
