
ARG REDIS_VER=6.2.4
ARG PYTHON_VER=3

FROM redisfab/redis:${REDIS_VER}-x64-buster as redis
FROM redislabs/redisearch:edge as search
FROM python:${PYTHON_VER}

ARG REDIS_VER
ARG PYTHON_VER

COPY --from=redis /usr/local/bin/redis* /usr/local/bin/
COPY --from=search /usr/lib/redis/ /usr/lib/redis/

# RUN set -e ;\
# 	apt-get -qq update ;\
# 	apt-get install -y git

WORKDIR /build

ADD ./ /build

RUN python --version
RUN pip install --quiet -r .circleci/circle_requirements.txt

RUN poetry install --no-dev
RUN poetry config virtualenvs.create false
RUN poetry build 

RUN pip install --force-reinstall git+https://github.com/RedisLabs/rmtest.git
RUN pip install pudb
			
ENV REDIS_PORT=6379

ENTRYPOINT ["bash", "-c", "/build/test/docker/redis-server.sh"]
