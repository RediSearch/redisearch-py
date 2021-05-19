FROM redislabs/redisearch:edge as builder

RUN apt update && apt install -y python3 python3-pip
ADD . /build
WORKDIR /build
RUN pip3 install poetry
RUN poetry config virtualenvs.create false
RUN poetry build

### clean docker stage
FROM redislabs/redisearch:edge as runner

RUN apt update && apt install -y python3 python3-pip git
RUN rm -rf /var/cache/apt/

COPY --from=builder /build/dist/redisearch*.tar.gz /tmp/
RUN pip3 install /tmp/redisearch*.tar.gz
