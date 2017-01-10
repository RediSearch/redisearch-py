# RediSearch Python Client

This is a python search engine library that utilizes the RediSearch Redis Module API.

It is the "official" client of redisearch, and should be regarded as its canonical client implementation.

## Features

RediSearch is an open-source (AGPL), [high performance search engine implemented as a Redis Module](http://redisearch.io). 
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
* Supports any utf-8 encoded text.
* Retrieving full document content or just ids
* Automatically index existing HASH keys as documents.

For more details, visit [http://redisearch.io](http://redisearch.io)

## Example: Using the Python Client

```py
# Creating a client with a given index name
client = Client('myIndex', port=conn.port)

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

## Installing

1. Install redis 4.0 RC2 or above

2. [Install and RediSearch] (http://redisearch.io/Quick_Start/#building-and-running)

3. Install the python client

```sh
$ pip install redisearch
```



