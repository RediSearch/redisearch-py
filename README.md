[![license](https://img.shields.io/github/license/RediSearch/redisearch-py.svg)](https://github.com/RediSearch/redisearch-py/blob/master/LICENSE)
[![PyPI version](https://badge.fury.io/py/redisearch.svg)](https://badge.fury.io/py/redisearch)
[![CircleCI](https://circleci.com/gh/RediSearch/redisearch-py/tree/master.svg?style=svg)](https://circleci.com/gh/RediSearch/redisearch-py/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RediSearch/redisearch-py.svg)](https://github.com/RediSearch/redisearch-py/releases/latest)
[![Codecov](https://codecov.io/gh/RediSearch/redisearch-py/branch/master/graph/badge.svg)](https://codecov.io/gh/RediSearch/redisearch-py)
[![Known Vulnerabilities](https://snyk.io/test/github/RediSearch/redisearch-py/badge.svg?targetFile=requirements.txt)](https://snyk.io/test/github/RediSearch/redisearch-py?targetFile=requirements.txt)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/RediSearch/redisearch-py.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/RediSearch/redisearch-py/alerts/)

# RediSearch Python Client
[![Forum](https://img.shields.io/badge/Forum-RediSearch-blue)](https://forum.redislabs.com/c/modules/redisearch/)
[![Gitter](https://badges.gitter.im/RedisLabs/RediSearch.svg)](https://gitter.im/RedisLabs/RediSearch?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

This is a python search engine library that utilizes the [RediSearch Redis Module](http://redisearch.io) API.

It is the "official" client of redisearch, and should be regarded as its canonical client implementation.

## Features

RediSearch is a source avaliable ([RSAL](https://raw.githubusercontent.com/RediSearch/RediSearch/master/LICENSE)), high performance search engine implemented as a [Redis Module](https://redis.io/topics/modules-intro). 
It uses custom data types to allow fast, stable and feature rich full-text search inside redis.

This client is a wrapper around the RediSearch API protocol, that allows you to utilize its features easily. 

### RediSearch's features include:

* Full-Text indexing of multiple fields in documents.
* Incremental indexing without performance loss.
* Document ranking (provided manually by the user at index time) and field weights.
* Auto-complete suggestions (with fuzzy prefix suggestions)
* Exact Phrase Search
* Stemming based query expansion in [many languages](http://redisearch.io/Stemming/) (using [Snowball](http://snowballstem.org/)).
* Limiting searches to specific document fields (up to 8 fields supported).
* Numeric filters and ranges.
* Automatically index existing HASH keys as documents.

For more details, visit [http://redisearch.io](http://redisearch.io)

## Example: Using the Python Client

```py
from redisearch import Client, TextField, IndexDefinition, Query

# Creating a client with a given index name
client = Client("myIndex")

# IndexDefinition is avaliable for RediSearch 2.0+
definition = IndexDefinition(prefix=['doc:', 'article:'])

# Creating the index definition and schema
client.create_index((TextField("title", weight=5.0), TextField("body")), definition=definition)

# Indexing a document for RediSearch 2.0+
client.redis.hset('doc:1',
                mapping={
                    'title': 'RediSearch',
                    'body': 'Redisearch impements a search engine on top of redis'
                })

# Indexing a document for RediSearch 1.x
client.add_document(
    "doc:2",
    title="RediSearch",
    body="Redisearch implements a search engine on top of redis",
)

# Simple search
res = client.search("search engine")

# the result has the total number of results, and a list of documents
print(res.total) # "2"
print(res.docs[0].title) # "RediSearch"

# Searching with complex parameters:
q = Query("search engine").verbatim().no_content().with_scores().paging(0, 5)
res = client.search(q)
```

## Installing

1. [Install RediSearch](http://redisearch.io/Quick_Start
2. Install the python client:

```sh
$ pip install redisearch
```

## Testing

Testing can easily be performed using using Docker.
Run the following:

```
make -C test/docker test PYTHON_VER=3
```

(Replace `PYTHON_VER=3` with `PYTHON_VER=2` to test with Python 2.7.)

Alternatively, use the following procedure:

First, run:

```
PYTHON_VER=3 ./test/test-setup.sh
```

This will set up a Python virtual environment in `venv3` (or in `venv2` if `PYTHON_VER=2` is used).

Afterwards, run RediSearch in a container as a daemon:

```
docker run -d -p 6379:6379 redislabs/redisearch:2.0.0
```

Finally, invoke the virtual environment and run the tests:

```
. ./venv3/bin/activate
REDIS_PORT=6379 python test/test.py 
REDIS_PORT=6379 python test/test_builder.py
```
