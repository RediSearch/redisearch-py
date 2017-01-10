from redis import Redis, RedisError, ConnectionPool
import itertools
import time
from .document import Document
from .result import Result


class Field(object):

    NUMERIC = 'NUMERIC'
    TEXT = 'TEXT'
    WEIGHT = 'WEIGHT'


    def __init__(self, name, *args):
        self.name = name
        self.args = args

    def redis_args(self):

        return [self.name] + list(self.args)

class TextField(Field):
    """
    TextField is used to define a text field in a schema definition
    """
    def __init__(self, name, weight = 1.0):
        Field.__init__(self, name, Field.TEXT, Field.WEIGHT, weight)

class NumericField(Field):
    """
    NumericField is used to define a numeric field in a schema defintion
    """

    def __init__(self, name):
        Field.__init__(self, name, Field.NUMERIC)


class Client(object):
    """
    A client for the RediSearch module. 
    It abstracts the API of the module and lets you just use the engine 
    """

    NUMERIC = 'numeric'

    CREATE_CMD = 'FT.CREATE'
    SEARCH_CMD = 'FT.SEARCH'
    ADD_CMD = 'FT.ADD'
    DROP_CMD = 'FT.DROP'

    


    class BatchIndexer(object):
        """
        A batch indexer allows you to automatically batch 
        document indexeing in pipelines, flushing it every N documents. 
        """

        def __init__(self, client, chunk_size = 1000):

            self.client = client
            self.pipeline = client.redis.pipeline(False)
            self.total = 0
            self.chunk_size = chunk_size
            self.current_chunk = 0

        def __del__(self):
            if self.current_chunk:
                self.commit()
        
        def add_document(self, doc_id, nosave = False, score=1.0, **fields):
            """
            Add a document to the batch query
            """
            self.client._add_document(doc_id, conn=self.pipeline, nosave = nosave, score = score, **fields)
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

    def __init__(self, index_name, host='localhost', port=6379):
        self.host = host
        self.port = port
        self.index_name = index_name

        self.redis = Redis(
            connection_pool = ConnectionPool(host=host, port=port))

    def batch_indexer(self, chunk_size = 100):
        """
        Create a new batch indexer from the client with a given chunk size
        """
        return Client.BatchIndexer(self, chunk_size = chunk_size)
    
    def create_index(self, *fields):
        """
        Create the search index. Creating an existing index juts updates its properties
        :param fields: a list of TextField or NumericField objects
        :return:
        """
        self.redis.execute_command(
            self.CREATE_CMD, self.index_name, 'SCHEMA', *itertools.chain(*(f.redis_args() for f in fields)))

    def drop_index(self):
        """
        Drop the index if it exists
        :return:
        """
        self.redis.execute_command(self.DROP_CMD, self.index_name)

    def _add_document(self, doc_id, conn = None, nosave = False, score=1.0, **fields):
        """ 
        Internal add_document used for both batch and single doc indexing 
        """
        if conn is None:
            conn = self.redis

        args = [self.ADD_CMD, self.index_name, doc_id, score]
        if nosave:
            args.append('NOSAVE')
        args.append('FIELDS') 
        args += list(itertools.chain(*fields.items()))
        return conn.execute_command(*args)

    def add_document(self, doc_id, nosave = False, score=1.0, **fields):
        """
        Add a single document to the index.
        :param doc_id: the id of the saved document.
        :param nosave: if set to true, we just index the document, and don't save a copy of it. 
                       this means that searches will just return ids.
        :param score: the document ranking, between 0.0 and 1.0. 
        :fields: kwargs dictionary of the document fields to be saved and/or indexed 
        """
        return self._add_document(doc_id, conn=None, nosave=nosave, score=score, **fields)

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

        res = self.redis.execute_command('FT.INFO', self.index_name)
        
        return {res[i]: res[i+1] for i in range(0, len(res), 2)}

    def search(self, query, offset =0, num = 10, verbatim = False, no_content=False,
               no_stopwords = False, fields=None, snippet_size = 500, **filters):
        """
        Search eht
        :param query:
        :param fields:
        :param filters:
        :return:
        """

        args = [self.index_name, query]
        if no_content:
            args.append('NOCONTENT')

        if fields:

            args.append('INFIELDS')
            args.append(len(fields))
            args += fields
        
        if verbatim:
            args.append('VERBATIM')

        if no_stopwords:
            args.append('NOSTOPWORDS')

        if filters:
            for k, v in filters.iteritems():
                args += ['FILTER', k] + list(v)

        args += ["LIMIT", offset, num]

        st = time.time()
        res = self.redis.execute_command(self.SEARCH_CMD, *args)

        return Result(res,  not no_content, queryText=query, snippet_size=snippet_size, duration = (time.time()-st)*1000.0)
