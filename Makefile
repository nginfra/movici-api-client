unittest:
	pytest -v tests/

flake8:
	flake8

black-check:
	black --check .

isort:
	isort .
	
isort-check:
	isort -c .

lint: flake8 black-check isort-check
	
test-all: lint unittest

level=patch
export level

bump-version:
	bumpversion  --config-file .bumpversion.app $(level)
	@NEW_VERSION=$$(tail -1 VERSION);\
	echo New version: $$NEW_VERSION

.PHONY: docs
docs:
	cd docs/ && $(MAKE) html SPHINXOPTS="-W --keep-going"

doctest:
	cd docs/ && $(MAKE) doctest