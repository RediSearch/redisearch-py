"""

## Overview 

`redisearch-py` is a python search engine library that utilizes the RediSearch Redis Module API.

It is the "official" client of redisearch, and should be regarded as its canonical client implementation.

### Example: Using the Python Client

```py
# Creating a client with a given index name
client = Client('myIndex')

# Creating the index definition and schema
client.create_index(TextField('title', weight=5.0), TextField('body'))

# Indexing a document
client.add_document('doc1', title = 'RediSearch', body = 'Redisearch impements a search engine on top of redis')

# Searching
res = client.search("search engine")

# the result has the total number of results, and a list of documents
print res.total # "1"
print res.docs[0].title 

```

### Installing

1. Install redis 4.0 RC2 or above

2. [Install and RediSearch] (http://redisearch.io/Quick_Start/#building-and-running)

3. Install the python client

```sh
$ pip install redisearch
```
"""
from .result import Result
from .document import Document
from .client import Client, NumericField, TextField



