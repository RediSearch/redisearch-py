"""

## Overview 

`redisearch-py` is a python search engine library that utilizes the RediSearch Redis Module API.

It is the "official" client of redisearch, and should be regarded as its canonical client implementation.

The source code can be found at [http://github.com/RedisLabs/redisearch-py](http://github.com/RedisLabs/redisearch-py)

### Example: Using the Python Client

```py

from redisearch import Client, TextField, NumericField

# Creating a client with a given index name
client = Client('myIndex')

# Creating the index definition and schema
client.create_index([TextField('title', weight=5.0), TextField('body')])

# Indexing a document
client.add_document('doc1', title = 'RediSearch', body = 'Redisearch impements a search engine on top of redis')

# Simple search
res = client.search("search engine")

# the result has the total number of results, and a list of documents
print res.total # "1"
print res.docs[0].title 

# Searching with snippets
res = client.search("search engine", snippet_sizes = {'body': 50})

# Searching with complext parameters:
q = Query("search engine").verbatim().no_content().paging(0,5)
res = client.search(q)

```

### Example: Using the Auto Completer Client:

```py

# Using the auto-completer
ac = AutoCompleter('ac')

# Adding some terms
ac.add_suggestions(Suggestion('foo', 5.0), Suggestion('bar', 1.0))

# Getting suggestions
suggs = ac.get_suggestions('goo') # returns nothing

suggs = ac.get_suggestions('goo', fuzzy = True) # returns ['foo']

```

### Installing

1. Install redis 4.0 RC2 or above

2. [Install RediSearch](http://redisearch.io/Quick_Start/#building-and-running)

3. Install the python client

```sh
$ pip install redisearch
```
"""
from .result import Result
from .document import Document
from .client import Client, NumericField, TextField, GeoField
from .query import Query, NumericFilter, GeoFilter
from .auto_complete import AutoCompleter, Suggestion


