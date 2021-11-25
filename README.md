[![license](https://img.shields.io/github/license/RediSearch/redisearch-py.svg)](https://github.com/RediSearch/redisearch-py/blob/master/LICENSE)
[![PyPI version](https://badge.fury.io/py/redisearch.svg)](https://badge.fury.io/py/redisearch)
[![CircleCI](https://circleci.com/gh/RediSearch/redisearch-py/tree/master.svg?style=svg)](https://circleci.com/gh/RediSearch/redisearch-py/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RediSearch/redisearch-py.svg)](https://github.com/RediSearch/redisearch-py/releases/latest)
[![Codecov](https://codecov.io/gh/RediSearch/redisearch-py/branch/master/graph/badge.svg)](https://codecov.io/gh/RediSearch/redisearch-py)
[![Known Vulnerabilities](https://snyk.io/test/github/RediSearch/redisearch-py/badge.svg?targetFile=pyproject.toml)](https://snyk.io/test/github/RediSearch/redisearch-py?targetFile=pyproject.toml)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/RediSearch/redisearch-py.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/RediSearch/redisearch-py/alerts/)

# RediSearch Python Client
[![Forum](https://img.shields.io/badge/Forum-RediSearch-blue)](https://forum.redislabs.com/c/modules/redisearch/)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/xTbqgTB)

## Deprecation notice

As of [redis-py 4.0.0](https://pypi.org/project/redis/4.0.0) this library is deprecated. It's features have been merged into redis-py. Please either install it [from pypy](https://pypi.org/project/redis) or [the repo](https://github.com/redis/redis-py).

--------------------------------

This is a Python search engine library that utilizes the [RediSearch Redis Module](http://redisearch.io) API.

It is the "official" client of RediSearch, and should be regarded as its canonical client implementation.

## Features

RediSearch is a source avaliable ([RSAL](https://raw.githubusercontent.com/RediSearch/RediSearch/master/LICENSE)), high performance search engine implemented as a [Redis Module](https://redis.io/topics/modules-intro).
It uses custom data types to allow fast, stable and feature rich full-text search inside Redis.

This client is a wrapper around the RediSearch API protocol, that allows you to utilize its features easily.

### RediSearch's features include:

* Full-Text indexing of multiple fields in documents.
* Incremental indexing without performance loss.
* Document ranking (provided manually by the user at index time) and field weights.
* Auto-complete suggestions (with fuzzy prefix suggestions).
* Exact Phrase Search.
* Stemming based query expansion in [many languages](http://redisearch.io/Stemming/) (using [Snowball](http://snowballstem.org/)).
* Limiting searches to specific document fields (up to 8 fields supported).
* Numeric filters and ranges.
* Automatically index existing HASH keys as documents.

For more details, visit [http://redisearch.io](http://redisearch.io)

## Examples

### Creating a client instance

When you create a redisearch-py client instance, the only required argument
is the name of the index.

```py
from redisearch import Client

client = Client("my-index")
```

To connect with a username and/or password, pass those options to the client
initializer.

```py
client = Client("my-index", password="my-password")
```

### Using core Redis commands

Every instance of `Client` contains an instance of the redis-py `Client` as
well. Use this object to run core Redis commands.

```py
import datetime

from redisearch import Client

START_TIME = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M.%S")

client = Client("my-index")

client.redis.set("start-time", START_TIME)
```

### Checking if a RediSearch index exists

To check if a RediSearch index exists, use the `FT.INFO` command and catch
the `ResponseError` raised if the index does not exist.

```py
from redis import ResponseError
from redisearch import Client

client = Client("my-index")

try:
    client.info()
except ResponseError:
    # Index does not exist. We need to create it!
```

### Defining a search index

Use an instance of `IndexDefinition` to define a search index. You only need
to do this when you create an index.

RediSearch indexes follow Hashes in your Redis databases by watching *key
prefixes*. If a Hash whose key starts with one of the search index's
configured key prefixes is added, updated, or deleted from Redis, RediSearch
will make those changes in the index. You configure a search index's key
prefixes using the `prefix` parameter of the `IndexDefinition` initializer.

**NOTE**: Once you create an index, RediSearch will continuously index these
keys when their Hashes change.

`IndexDefinition` also takes a *schema*. The schema specifies which fields to
index from within the Hashes that the index follows. The field types are:

* TextField
* TagField
* NumericField
* GeoField

For more information on what these field types mean, consult the [RediSearch
documentation](https://oss.redislabs.com/redisearch/Commands/#ftcreate) on
the `FT.CREATE` command.

With redisearch-py, the schema is an iterable of `Field` instances. Once you
have an `IndexDefinition` instance, you can create the instance by passing a
schema iterable to the `create_index()` method.

```py
from redis import ResponseError
from redisearch import Client, IndexDefinition, TextField

SCHEMA = (
    TextField("title", weight=5.0),
    TextField("body")
)

client = Client("my-index")

definition = IndexDefinition(prefix=['blog:'])

try:
    client.info()
except ResponseError:
    # Index does not exist. We need to create it!
    client.create_index(SCHEMA, definition=definition)
```

### Indexing a document

A RediSearch 2.0 index continually follows Hashes with the key prefixes you
defined, so if you want to add a document to the index, you only need to
create a Hash with one of those prefixes.

```py
# Indexing a document with RediSearch 2.0.
doc = {
    'title': 'RediSearch',
    'body': 'Redisearch adds querying, indexing, and full-text search to Redis'
}
client.redis.hset('doc:1', mapping=doc)
```

Past versions of RediSearch required that you call the `add_document()`
method. This method is deprecated, but we include its usage here for
reference.

```py
# Indexing a document for RediSearch 1.x
client.add_document(
    "doc:2",
    title="RediSearch",
    body="Redisearch implements a search engine on top of redis",
)
```

### Querying

#### Basic queries

Use the `search()` method to perform basic full-text and field-specific
searches. This method doesn't take many of the options available to the
RediSearch `FT.SEARCH` command -- read the section on building complex
queries later in this document for information on how to use those.

```py
res = client.search("evil wizards")
```
#### Result objects

Results are wrapped in a `Result` object that includes the number of results
and a list of matching documents.

```py
>>> print(res.total)
2
>>> print(res.docs[0].title)
"Wizard Story 2: Evil Wizards Strike Back"
```

#### Building complex queries

You can use the `Query` object to build complex queries:

```py
q = Query("evil wizards").verbatim().no_content().with_scores().paging(0, 5)
res = client.search(q)
```

 For an explanation of these options, see the [RediSearch
 documentation](https://oss.redislabs.com/redisearch/Commands/#ftsearch) for
 the `FT.SEARCH` command.

#### Query syntax

The default behavior of queries is to run a full-text search across all
`TEXT` fields in the index for the intersection of all terms in the query.

So the example given in the "Basic queries" section of this README,
`client.search("evil wizards")`, run a full-text search for the intersection
of "evil" and "wizard" in all `TEXT` fields.

Many more types of queries are possible, however! The string you pass into
the `search()` method or `Query()` initializer has the full range of query
syntax available in RediSearch.

For example, a full-text search against a specific `TEXT` field in the index
looks like this:

```py
# Full-text search
res = client.search("@title:evil wizards")
```

Finding books published in 2020 or 2021 looks like this:

```python
client.search("@published_year:[2020 2021]")
```

To learn more, see the [RediSearch
documentation](https://oss.redislabs.com/redisearch/Query_Syntax/) on query
syntax.

### Aggregations

This library contains a programmatic interface to run [aggregation
queries](https://oss.redislabs.com/redisearch/Aggregations/) with RediSearch.

#### Making an aggregation query

To make an aggregation query, pass an instance of the `AggregateRequest`
class to the `search()` method of an instance of `Client`.

For example, here is what finding the most books published in a single year
looks like:

```py
from redisearch import Client
from redisearch import reducers
from redisearch.aggregation import AggregateRequest

client = Client('books-idx')

request = AggregateRequest('*').group_by(
    '@published_year', reducers.count().alias("num_published")
).group_by(
    [], reducers.max("@num_published").alias("max_books_published_per_year")
)

result = client.aggregate(request)
```

#### A redis-cli equivalent query

The aggregation query just given is equivalent to the following
`FT.AGGREGATE` command entered directly into the redis-cli:

```sql
FT.AGGREGATE books-idx *
    GROUPBY 1 @published_year
      REDUCE COUNT 0 AS num_published
    GROUPBY 0
      REDUCE MAX 1 @num_published AS max_books_published_per_year
```

#### The AggregateResult object

Aggregation queries return an `AggregateResult` object that contains the rows
returned for the query and a cursor if you're using the [cursor
API](https://oss.redislabs.com/redisearch/Aggregations/#cursor_api).

```py
from redisearch.aggregation import AggregateRequest, Asc

request = AggregateRequest('*').group_by(
    ['@published_year'], reducers.avg('average_rating').alias('average_rating_for_year')
).sort_by(
    Asc('@average_rating_for_year')
).limit(
    0, 10
).filter('@published_year > 0')

...


In [53]: resp = c.aggregate(request)
In [54]: resp.rows
Out[54]:
[['published_year', '1914', 'average_rating_for_year', '0'],
 ['published_year', '2009', 'average_rating_for_year', '1.39166666667'],
 ['published_year', '2011', 'average_rating_for_year', '2.046'],
 ['published_year', '2010', 'average_rating_for_year', '3.125'],
 ['published_year', '2012', 'average_rating_for_year', '3.41'],
 ['published_year', '1967', 'average_rating_for_year', '3.603'],
 ['published_year', '1970', 'average_rating_for_year', '3.71875'],
 ['published_year', '1966', 'average_rating_for_year', '3.72666666667'],
 ['published_year', '1927', 'average_rating_for_year', '3.77']]
```

#### Reducer functions

Notice from the example that we used an object from the `reducers` module.
See the [RediSearch documentation](https://oss.redislabs.com/redisearch/Aggregations/#groupby_reducers)
for more examples of reducer functions you can use when grouping results.

Reducer functions include an `alias()` method that gives the result of the
reducer a specific name. If you don't supply a name, RediSearch will generate
one.

#### Grouping by zero, one, or multiple fields

The `group_by` statement can take a single field name as a string, or multiple
field names as a list of strings.

```py
AggregateRequest('*').group_by('@published_year', reducers.count())

AggregateRequest('*').group_by(
    ['@published_year', '@average_rating'],
    reducers.count())
```

To run a reducer function on every result from an aggregation query, pass an
empty list to `group_by()`, which is equivalent to passing the option
`GROUPBY 0` when writing an aggregation in the redis-cli.

```py
AggregateRequest('*').group_by([], reducers.max("@num_published"))
```

**NOTE**: Aggregation queries require at least one `group_by()` method call.

#### Sorting and limiting

Using an `AggregateRequest` instance, you can sort with the `sort_by()` method
and limit with the `limit()` method.

For example, finding the average rating of books published each year, sorting
by the average rating for the year, and returning only the first ten results:

```py
from redisearch import Client
from redisearch.aggregation import AggregateRequest, Asc

c = Client()

request = AggregateRequest('*').group_by(
    ['@published_year'], reducers.avg('average_rating').alias('average_rating_for_year')
).sort_by(
    Asc('@average_rating_for_year')
).limit(0, 10)

c.aggregate(request)
```

**NOTE**: The first option to `limit()` is a zero-based offset, and the second
option is the number of results to return.

#### Filtering

Use filtering to reject results of an aggregation query after your reducer
functions run. For example, calculating the average rating of books published
each year and only returning years with an average rating higher than 3:

```py
from redisearch.aggregation import AggregateRequest, Asc

req = AggregateRequest('*').group_by(
    ['@published_year'], reducers.avg('average_rating').alias('average_rating_for_year')
).sort_by(
    Asc('@average_rating_for_year')
).filter('@average_rating_for_year > 3')
```

## Installing

1. [Install RediSearch](http://redisearch.io/Quick_Start)
2. Install the Python client:

```sh
$ pip install redisearch
```

## Developing

1. Create a virtualenv to manage your python dependencies, and ensure it's active.
   ```virtualenv -v venv```
2. Install [pypoetry](https://python-poetry.org/) to manage your dependencies.
   ```pip install --user poetry```
3. Install dependencies.
   ```poetry install```

Note: Due to an [interaction between](https://github.com/python-poetry/poetry/issues/4210) and python 3.10, you *may* need to run the following, if you receive a JSONError while installing packages.
```
poetry config experimental.new-installer false
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
