
ARG PYTHON_VER

FROM redislabs/redisearch:2.0.0

RUN set -e ;\
	apt-get -qq update ;\
	apt-get install -y git

WORKDIR /build

RUN set -e ;\
	mkdir -p deps ;\
	cd deps ;\
	git clone https://github.com/RedisLabsModules/readies.git

RUN if [ "$PYTHON_VER" = 2 ]; then \
		PIP=1 ./deps/readies/bin/getpy2 ;\
		python2 --version ;\
	else \
		PIP=1 ./deps/readies/bin/getpy3 ;\
		python3 --version ;\
	fi

ADD ./ /build

RUN pip install -r requirements.txt
RUN pip install --force-reinstall git+https://github.com/RedisLabs/rmtest.git

ENV REDIS_PORT=6379

ENTRYPOINT [ "/bin/bash", "-c", "/build/test/docker/test.sh" ]
