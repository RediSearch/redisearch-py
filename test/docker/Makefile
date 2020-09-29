
.PHONY: all build test

PYTHON_VER ?= 3

all: test

build:
	docker build --no-cache -t redisearch-py-test -f Dockerfile --build-arg PYTHON_VER=$(PYTHON_VER) ../..

test: build
	docker run --rm -it redisearch-py-test
