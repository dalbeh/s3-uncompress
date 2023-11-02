#!make

include .env
export

init:
	poetry config virtualenvs.in-project true && poetry init 

dev:
	poetry config virtualenvs.in-project true && poetry install

pro:
	poetry config virtualenvs.in-project true && poetry install --without dev

unit-tests:
	poetry run python3 -m pytest --cov-report=term-missing --cov-report html --cov=s3_uncompress -s --log-level=INFO --durations=0 -vv

tox:
	rm -rf .tox && poetry run tox

destroy:
	poetry env info -p && rm -rf `poetry env info -p`

add_dev:
	poetry add $(lib) --dev 