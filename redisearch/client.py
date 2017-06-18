from redis import Redis, RedisError, ConnectionPool
import itertools
import time
from .document import Document
from .result import Result
from .query import Query, Filter


class Field(object):

    NUMERIC = 'NUMERIC'
    TEXT = 'TEXT'
    WEIGHT = 'WEIGHT'
    GEO = 'GEO'
    SORTABLE = 'SORTABLE'

    def __init__(self, name, *args):
        self.name = name
        self.args = args
    
    def redis_args(self):

        return [self.name] + list(self.args)


class TextField(Field):
    """
    TextField is used to define a text field in a schema definition
    """

    def __init__(self, name, weight=1.0, sortable = False):
        args = [Field.TEXT, Field.WEIGHT, weight]
        if sortable:
            args.append(Field.SORTABLE)

        Field.__init__(self, name, *args)


class NumericField(Field):
    """
    NumericField is used to define a numeric field in a schema defintion
    """

    def __init__(self, name, sortable = False):
        if sortable:
            Field.__init__(self, name, Field.NUMERIC, Field.SORTABLE)
        else:
            Field.__init__(self, name, Field.NUMERIC)


class GeoField(Field):
    """
    GeoField is used to define a geo-indexing field in a schema defintion
    """

    def __init__(self, name):
        Field.__init__(self, name, Field.GEO)


class Client(object):
    """
    A client for the RediSearch module. 
    It abstracts the API of the module and lets you just use the engine 
    """

    NUMERIC = 'NUMERIC'

    CREATE_CMD = 'FT.CREATE'
    SEARCH_CMD = 'FT.SEARCH'
    ADD_CMD = 'FT.ADD'
    DROP_CMD = 'FT.DROP'
    EXPLAIN_CMD = 'FT.EXPLAIN'

    NOOFFSETS = 'NOOFFSETS'
    NOFIELDS = 'NOFIELDS'
    NOSCOREIDX = 'NOSCOREIDX'
    STOPWORDS = 'STOPWORDS'

    class BatchIndexer(object):
        """
        A batch indexer allows you to automatically batch 
        document indexeing in pipelines, flushing it every N documents. 
        """

        def __init__(self, client, chunk_size=1000):

            self.client = client
            self.pipeline = client.redis.pipeline(False)
            self.total = 0
            self.chunk_size = chunk_size
            self.current_chunk = 0

        def __del__(self):
            if self.current_chunk:
                self.commit()

        def add_document(self, doc_id, nosave=False, score=1.0, payload=None, replace=False, **fields):
            """
            Add a document to the batch query
            """
            self.client._add_document(doc_id, conn=self.pipeline, nosave=nosave, score=score,
                                      payload=payload, replace=replace, **fields)
            self.current_chunk += 1
            self.total += 1
            if self.current_chunk >= self.chunk_size:
                self.commit()

        def commit(self):
            """
            Manually commit and flush the batch indexing query
            """
            self.pipeline.execute()
            self.current_chunk = 0

    def __init__(self, index_name, host='localhost', port=6379, conn=None):
        """
        Create a new Client for the given index_name, and optional host and port

        If conn is not None, we employ an already existing redis connection
        """

        self.index_name = index_name

        self.redis = conn if conn is not None else Redis(
            connection_pool=ConnectionPool(host=host, port=port))

    def batch_indexer(self, chunk_size=100):
        """
        Create a new batch indexer from the client with a given chunk size
        """
        return Client.BatchIndexer(self, chunk_size=chunk_size)

    def create_index(self, fields, no_term_offsets=False,
                     no_field_flags=False, no_score_indexes=False, stopwords = None):
        """
        Create the search index. Creating an existing index juts updates its properties

        ### Parameters:

        - **fields**: a list of TextField or NumericField objects
        - **no_term_offsets**: If true, we will not save term offsets in the index
        - **no_field_flags**: If true, we will not save field flags that allow searching in specific fields
        - **no_score_indexes**: If true, we will not save optimized top score indexes for single word queries
        - **stopwords**: If not None, we create the index with this custom stopword list. The list can be empty
        """

        args = [self.CREATE_CMD, self.index_name]
        if no_term_offsets:
            args.append(self.NOOFFSETS)
        if no_field_flags:
            args.append(self.NOFIELDS)
        if no_score_indexes:
            args.append(self.NOSCOREIDX)
        if stopwords is not None and isinstance(stopwords, (list, tuple, set)):
            args += [self.STOPWORDS, len(stopwords)]
            if len(stopwords) > 0:
                args += list(stopwords)
    
        args.append('SCHEMA')

        args += list(itertools.chain(*(f.redis_args() for f in fields)))

        return self.redis.execute_command(*args)

    def drop_index(self):
        """
        Drop the index if it exists
        """
        return self.redis.execute_command(self.DROP_CMD, self.index_name)

    def _add_document(self, doc_id, conn=None, nosave=False, score=1.0, payload=None,
                      replace=False, **fields):
        """ 
        Internal add_document used for both batch and single doc indexing 
        """
        if conn is None:
            conn = self.redis

        args = [self.ADD_CMD, self.index_name, doc_id, score]
        if nosave:
            args.append('NOSAVE')
        if payload is not None:
            args.append('PAYLOAD')
            args.append(payload)
        if replace:
            args.append('REPLACE')
        args.append('FIELDS')
        args += list(itertools.chain(*fields.items()))
        return conn.execute_command(*args)

    def add_document(self, doc_id, nosave=False, score=1.0, payload=None, replace=False, **fields):
        """
        Add a single document to the index.

        ### Parameters

        - **doc_id**: the id of the saved document.
        - **nosave**: if set to true, we just index the document, and don't save a copy of it. This means that searches will just return ids.
        - **score**: the document ranking, between 0.0 and 1.0 
        - **payload**: optional inner-index payload we can save for fast access in scoring functions
        - **replace**: if True, and the document already is in the index, we perform an update and reindex the document
        - **fields** kwargs dictionary of the document fields to be saved and/or indexed. 
                     NOTE: Geo points shoule be encoded as strings of "lon,lat"
        """
        return self._add_document(doc_id, conn=None, nosave=nosave, score=score, 
                                  payload=payload, replace=replace, **fields)

    def load_document(self, id):
        """
        Load a single document by id
        """
        fields = self.redis.hgetall(id)
        try:
            del fields['id']
        except KeyError:
            pass

        return Document(id=id, **fields)

    def info(self):
        """
        Get info an stats about the the current index, including the number of documents, memory consumption, etc
        """

        res = self.redis.execute_command('FT.INFO', self.index_name)

        return {res[i]: res[i + 1] for i in range(0, len(res), 2)}

    def search(self, query, snippet_sizes=None):
        """
        Search the index for a given query, and return a result of documents

        ### Parameters

        - **query**: the search query. Either a text for simple queries with default parameters, or a Query object for complex queries.
                     See RediSearch's documentation on query format
        - **snippet_sizes**: A dictionary of {field: snippet_size} used to trim and format the result. e.g.e {'body': 500}
        """

        args = [self.index_name]

        if isinstance(query, (str, unicode)):
            # convert the query from a text to a query object
            query = Query(query)
        if not isinstance(query, Query):
            raise ValueError("Bad query type %s" % type(query))

        args += query.get_args()
        query_text = query.query_string()

        st = time.time()
        res = self.redis.execute_command(self.SEARCH_CMD, *args)

        return Result(res, not query._no_content, query_text=query_text,
                      snippets=snippet_sizes, duration=(
                          time.time() - st) * 1000.0,
                      has_payload=query._with_payloads)
