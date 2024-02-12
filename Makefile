.PHONY: build

setup-glue-local:
	chmod +x automation/glue_setup.sh
	automation/glue_setup.sh

glue-demo-env:
	cp app/.custom_env .env

install:
	pip3 install -r requirements.txt

type-check:
	mypy ./ --ignore-missing-imports

lint:
	pylint app tests jobs setup.py

test:
	export KAGGLE_KEY=MOCKKEY
	export KAGGLE_USERNAME=MOCKUSERNAME
    coverage run --source=app -m unittest discover -s tests

coverage-report:
	coverage report
	coverage html
