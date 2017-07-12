IMAGE_NAME = astronomerio/airflow

all: build lint test
.PHONY: all

build:
	docker build -t $(IMAGE_NAME) .

test: build
	docker run -e AIRFLOW__CORE__UNIT_TEST_MODE=True -e AIRFLOW__CORE__EXECUTOR=SequentialExecutor $(IMAGE_NAME) nose2

lint: build
	docker run $(IMAGE_NAME) pycodestyle .

test-ci:
	docker run -e AIRFLOW__CORE__EXECUTOR=SequentialExecutor -e AIRFLOW__CORE__UNIT_TEST_MODE=True $(IMAGE_NAME) nose2

lint-ci:
	docker run $(IMAGE_NAME) pycodestyle .
