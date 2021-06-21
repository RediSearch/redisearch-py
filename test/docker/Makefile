
.PHONY: all build start sh test logs stop help

REDIS_VER ?= 6.2.4
PYTHON_VER ?= 3

IMG=redisearch-py-tests
# :py${PYTHON_VER}-redis${REDIS_VER}-search${}
CID=redisearch-py-tests.cid

define HELP
make build            # Build container
  PYTHON_VER=ver        # Python version
  REDIS_VER=ver         # Redis Server version
  DOCKER_ARGS="args"    # Extra docker build arguments
make start            # Start container
make stop             # Stop container
make test             # Run tests
make sh               # Invoke container
make log              # Show Redis Server logs

endef

all: build start test stop

build:
	docker build --no-cache -t $(IMG) -f Dockerfile \
		--build-arg PYTHON_VER=$(PYTHON_VER) \
		--build-arg REDIS_VER=$(REDIS_VER) \
		../..

start:
	@rm -f $(CID)
	docker run --cidfile=$(CID) -d --rm -it  $(DOCKER_ARGS) $(IMG)

sh:
	docker exec -it `cat $(CID)` bash

test:
	docker exec -it `cat $(CID)` /build/test/docker/test.sh

logs:
	docker logs `cat $(CID)`

stop:
	docker stop `cat $(CID)`
	@rm -f $(CID)

ifneq ($(HELP),)
ifneq ($(filter help,$(MAKECMDGOALS)),)
HELPFILE:=$(shell mktemp /tmp/make.help.XXXX)
endif
endif

help:
	$(file >$(HELPFILE),$(HELP))
	@echo
	@cat $(HELPFILE)
	@echo
	@-rm -f $(HELPFILE)
