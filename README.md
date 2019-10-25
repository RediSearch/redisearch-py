[![license](https://img.shields.io/github/license/RediSearch/redisearch-py.svg)](https://github.com/RediSearch/redisearch-py/blob/master/LICENSE)
[![PyPI version](https://badge.fury.io/py/redisearch.svg)](https://badge.fury.io/py/redisearch)
[![CircleCI](https://circleci.com/gh/RediSearch/redisearch-py/tree/master.svg?style=svg)](https://circleci.com/gh/RediSearch/redisearch-py/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RediSearch/redisearch-py.svg)](https://github.com/RediSearch/redisearch-py/releases/latest)
[![Codecov](https://codecov.io/gh/RediSearch/redisearch-py/branch/master/graph/badge.svg)](https://codecov.io/gh/RediSearch/redisearch-py)


# RediSearch Python Client
[![Mailing List](https://img.shields.io/badge/Mailing%20List-RediSearch-blue)](https://groups.google.com/forum/#!forum/redisearch)
[![Gitter](https://badges.gitter.im/RedisLabs/RediSearch.svg)](https://gitter.im/RedisLabs/RediSearch?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

This is a python search engine library that utilizes the [RediSearch Redis Module](http://redisearch.io) API.

It is the "official" client of redisearch, and should be regarded as its canonical client implementation.

## Features

RediSearch is an open-source (AGPL), high performance search engine implemented as a [Redis Module](https://redis.io/topics/modules-intro). 
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
# Creating a client with a given index name
client = Client('myIndex')

# Creating the index definition and schema
client.create_index((TextField('title', weight=5.0), TextField('body')))

# Indexing a document
client.add_document('doc1', title = 'RediSearch', body = 'Redisearch impements a search engine on top of redis')

# Simple search
res = client.search("search engine")

# Searching with snippets
res = client.search("search engine", snippet_sizes = {'body': 50})

# Searching with complext parameters:
q = Query("search engine").verbatim().no_content().paging(0,5)
res = client.search(q)


# the result has the total number of results, and a list of documents
print res.total # "1"
print res.docs[0].title 

```

## Installing

1. Install Redis 4.0 or above

2. [Install RediSearch](http://redisearch.io/Quick_Start/#building-and-running)

3. Install the python client

```sh
$ pip install redisearch
```



