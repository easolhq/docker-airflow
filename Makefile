IMAGE_NAME = astronomerio/airflow

all: build lint test
.PHONY: all

build:
	docker build -t $(IMAGE_NAME) .

test: build
	docker run $(IMAGE_NAME) nose2

lint: build
	docker run $(IMAGE_NAME) pycodestyle .

test-ci:
	docker run $(IMAGE_NAME) nose2

lint-ci:
	docker run $(IMAGE_NAME) pycodestyle .
