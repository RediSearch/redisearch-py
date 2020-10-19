from redis import Redis, ConnectionPool
import itertools
import time
import six

from .document import Document
from .result import Result
from .query import Query
from ._util import to_string
from .aggregation import AggregateRequest, AggregateResult, Cursor


class Field(object):

    NUMERIC = 'NUMERIC'
    TEXT = 'TEXT'
    WEIGHT = 'WEIGHT'
    GEO = 'GEO'
    TAG = 'TAG'
    SORTABLE = 'SORTABLE'
    NOINDEX = 'NOINDEX'
    SEPARATOR = 'SEPARATOR'
    PHONETIC = 'PHONETIC'

    def __init__(self, name, *args):
        self.name = name
        self.args = args

    def redis_args(self):
        return [self.name] + list(self.args)


class TextField(Field):
    """
    TextField is used to define a text field in a schema definition
    """
    NOSTEM = 'NOSTEM'

    def __init__(self, name, weight=1.0, sortable=False, no_stem=False,
                 no_index=False, phonetic_matcher=None):
        args = [Field.TEXT, Field.WEIGHT, weight]
        if no_stem:
            args.append(self.NOSTEM)
        if sortable:
            args.append(Field.SORTABLE)
        if no_index:
            args.append(self.NOINDEX)
        if phonetic_matcher and phonetic_matcher in ['dm:en', 'dm:fr', 'dm:pt', 'dm:es']:
            args.append(self.PHONETIC)
            args.append(phonetic_matcher)

        if no_index and not sortable:
            raise ValueError('Non-Sortable non-Indexable fields are ignored')
        Field.__init__(self, name, *args)


class NumericField(Field):
    """
    NumericField is used to define a numeric field in a schema defintion
    """

    def __init__(self, name, sortable=False, no_index=False):
        args = [Field.NUMERIC]
        if sortable:
            args.append(Field.SORTABLE)
        if no_index:
            args.append(Field.NOINDEX)

        if no_index and not sortable:
            raise ValueError('Non-Sortable non-Indexable fields are ignored')

        super(NumericField, self).__init__(name, *args)


class GeoField(Field):
    """
    GeoField is used to define a geo-indexing field in a schema defintion
    """

    def __init__(self, name):
        Field.__init__(self, name, Field.GEO)


class TagField(Field):
    """
    TagField is a tag-indexing field with simpler compression and tokenization. 
    See http://redisearch.io/Tags/
    """

    def __init__(self, name, separator=',', no_index=False):
        args = [Field.TAG, Field.SEPARATOR, separator]

        if no_index:
            args.append(Field.NOINDEX)

        Field.__init__(self, name, *args)


class IndexDefinition(object):
    """
    IndexDefinition is used to define a index definition for automatic indexing on Hash update
    """

    ON = 'ON'
    HASH = 'HASH'
    PREFIX = 'PREFIX'
    FILTER = 'FILTER'
    LANGUAGE_FIELD = 'LANGUAGE_FIELD'
    LANGUAGE = 'LANGUAGE'
    SCORE_FIELD = 'SCORE_FIELD'
    SCORE = 'SCORE'
    PAYLOAD_FIELD = 'PAYLOAD_FIELD'
        
    def __init__(self, prefix=[], filter=None, language_field=None, language=None, score_field=None, score=1.0, payload_field=None):

        args = [self.ON, self.HASH]

        if len(prefix) > 0:
            args.append(self.PREFIX)
            args.append(len(prefix))
            for p in prefix:
                args.append(p)

        if filter is not None:
            args.append(self.FILTER)
            args.append(filter)

        if language_field is not None:
            args.append(self.LANGUAGE_FIELD)
            args.append(language_field)

        if language is not None:
            args.append(self.LANGUAGE)
            args.append(language)

        if score_field is not None:
            args.append(self.SCORE_FIELD)
            args.append(score_field)

        if score is not None:
            args.append(self.SCORE)
            args.append(score)

        if payload_field is not None:
            args.append(self.PAYLOAD_FIELD)
            args.append(payload_field)

        self.args = args
    

class Client(object):
    """
    A client for the RediSearch module. 
    It abstracts the API of the module and lets you just use the engine 
    """

    NUMERIC = 'NUMERIC'

    CREATE_CMD = 'FT.CREATE'
    ALTER_CMD = 'FT.ALTER'
    SEARCH_CMD = 'FT.SEARCH'
    ADD_CMD = 'FT.ADD'
    ADDHASH_CMD = "FT.ADDHASH"
    DROP_CMD = 'FT.DROP'
    EXPLAIN_CMD = 'FT.EXPLAIN'
    DEL_CMD = 'FT.DEL'
    AGGREGATE_CMD = 'FT.AGGREGATE'
    CURSOR_CMD = 'FT.CURSOR'
    SPELLCHECK_CMD = 'FT.SPELLCHECK'
    DICT_ADD_CMD = 'FT.DICTADD'
    DICT_DEL_CMD = 'FT.DICTDEL'
    DICT_DUMP_CMD = 'FT.DICTDUMP'
    GET_CMD = 'FT.GET'
    MGET_CMD = 'FT.MGET'
    CONFIG_CMD = 'FT.CONFIG'
    TAGVALS_CMD = 'FT.TAGVALS'
    ALIAS_ADD_CMD = 'FT.ALIASADD'
    ALIAS_UPDATE_CMD = 'FT.ALIASUPDATE'
    ALIAS_DEL_CMD = 'FT.ALIASDEL'

    NOOFFSETS = 'NOOFFSETS'
    NOFIELDS = 'NOFIELDS'
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

        def add_document(self, doc_id, nosave=False, score=1.0, payload=None,
                         replace=False, partial=False, no_create=False, **fields):
            """
            Add a document to the batch query
            """
            self.client._add_document(doc_id, conn=self.pipeline, nosave=nosave, score=score,
                                      payload=payload, replace=replace,
                                      partial=partial, no_create=no_create, **fields)
            self.current_chunk += 1
            self.total += 1
            if self.current_chunk >= self.chunk_size:
                self.commit()

        def add_document_hash(
            self, doc_id, score=1.0, replace=False,
        ):
            """
            Add a hash to the batch query
            """
            self.client._add_document_hash(
                doc_id, conn=self.pipeline, score=score, replace=replace,
            )
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

    def __init__(self, index_name, host='localhost', port=6379, conn=None, password=None):
        """
        Create a new Client for the given index_name, and optional host and port

        If conn is not None, we employ an already existing redis connection
        """

        self.index_name = index_name

        self.redis = conn if conn is not None else Redis(
            connection_pool=ConnectionPool(host=host, port=port, password=password,
            decode_responses=True))

    def batch_indexer(self, chunk_size=100):
        """
        Create a new batch indexer from the client with a given chunk size
        """
        return Client.BatchIndexer(self, chunk_size=chunk_size)

    def create_index(self, fields, no_term_offsets=False,
                     no_field_flags=False, stopwords=None, definition=None):
        """
        Create the search index. The index must not already exist.

        ### Parameters:

        - **fields**: a list of TextField or NumericField objects
        - **no_term_offsets**: If true, we will not save term offsets in the index
        - **no_field_flags**: If true, we will not save field flags that allow searching in specific fields
        - **stopwords**: If not None, we create the index with this custom stopword list. The list can be empty
        """

        args = [self.CREATE_CMD, self.index_name]
        if definition is not None:
            args += definition.args            
        if no_term_offsets:
            args.append(self.NOOFFSETS)
        if no_field_flags:
            args.append(self.NOFIELDS)
        if stopwords is not None and isinstance(stopwords, (list, tuple, set)):
            args += [self.STOPWORDS, len(stopwords)]
            if len(stopwords) > 0:
                args += list(stopwords)

        args.append('SCHEMA')

        args += list(itertools.chain(*(f.redis_args() for f in fields)))

        return self.redis.execute_command(*args)

    def alter_schema_add(self, fields):
        """
        Alter the existing search index by adding new fields. The index must already exist.

        ### Parameters:

        - **fields**: a list of Field objects to add for the index
        """

        args = [self.ALTER_CMD, self.index_name, 'SCHEMA', 'ADD']

        args += list(itertools.chain(*(f.redis_args() for f in fields)))

        return self.redis.execute_command(*args)

    def drop_index(self):
        """
        Drop the index if it exists
        """
        return self.redis.execute_command(self.DROP_CMD, self.index_name)

    def _add_document(self, doc_id, conn=None, nosave=False, score=1.0, payload=None,
                      replace=False, partial=False, language=None, no_create=False, **fields):
        """ 
        Internal add_document used for both batch and single doc indexing 
        """
        if conn is None:
            conn = self.redis

        if partial or no_create:
            replace = True

        args = [self.ADD_CMD, self.index_name, doc_id, score]
        if nosave:
            args.append('NOSAVE')
        if payload is not None:
            args.append('PAYLOAD')
            args.append(payload)
        if replace:
            args.append('REPLACE')
            if partial:
                args.append('PARTIAL')
            if no_create:
                args.append('NOCREATE')
        if language:
            args += ['LANGUAGE', language]
        args.append('FIELDS')
        args += list(itertools.chain(*fields.items()))
        return conn.execute_command(*args)

    def _add_document_hash(
        self, doc_id, conn=None, score=1.0, language=None, replace=False,
    ):
        """ 
        Internal add_document_hash used for both batch and single doc indexing 
        """
        if conn is None:
            conn = self.redis

        args = [self.ADDHASH_CMD, self.index_name, doc_id, score]

        if replace:
            args.append("REPLACE")

        if language:
            args += ["LANGUAGE", language]

        return conn.execute_command(*args)

    def add_document(self, doc_id, nosave=False, score=1.0, payload=None,
                     replace=False, partial=False, language=None, no_create=False, **fields):
        """
        Add a single document to the index.

        ### Parameters

        - **doc_id**: the id of the saved document.
        - **nosave**: if set to true, we just index the document, and don't save a copy of it. This means that searches will just return ids.
        - **score**: the document ranking, between 0.0 and 1.0 
        - **payload**: optional inner-index payload we can save for fast access in scoring functions
        - **replace**: if True, and the document already is in the index, we perform an update and reindex the document
        - **partial**: if True, the fields specified will be added to the existing document.
                       This has the added benefit that any fields specified with `no_index`
                       will not be reindexed again. Implies `replace`
        - **language**: Specify the language used for document tokenization.
        - **no_create**: if True, the document is only updated and reindexed if it already exists. 
                         If the document does not exist, an error will be returned. Implies `replace`
        - **fields** kwargs dictionary of the document fields to be saved and/or indexed. 
                     NOTE: Geo points shoule be encoded as strings of "lon,lat"
        """
        return self._add_document(doc_id, conn=None, nosave=nosave, score=score,
                                  payload=payload, replace=replace,
                                  partial=partial, language=language,
                                  no_create=no_create, **fields)

    def add_document_hash(
        self, doc_id, score=1.0, language=None, replace=False,
    ):
        """
        Add a hash document to the index.

        ### Parameters

        - **doc_id**: the document's id. This has to be an existing HASH key in Redis that will hold the fields the index needs.
        - **score**: the document ranking, between 0.0 and 1.0 
        - **replace**: if True, and the document already is in the index, we perform an update and reindex the document
        - **language**: Specify the language used for document tokenization.
        """
        return self._add_document_hash(
            doc_id, conn=None, score=score, language=language, replace=replace,
        )

    def delete_document(self, doc_id, conn=None, delete_actual_document=False):
        """
        Delete a document from index
        Returns 1 if the document was deleted, 0 if not

        ### Parameters

        - **delete_actual_document**: if set to True, RediSearch also delete the actual document if it is in the index
        """
        args = [self.DEL_CMD, self.index_name, doc_id]
        if conn is None:
            conn = self.redis
        if delete_actual_document:
            args.append('DD')

        return conn.execute_command(*args)

    def load_document(self, id):
        """
        Load a single document by id
        """
        fields = self.redis.hgetall(id)
        if six.PY3:
            f2 = {to_string(k): to_string(v) for k, v in fields.items()}
            fields = f2

        try:
            del fields['id']
        except KeyError:
            pass

        return Document(id=id, **fields)

    def get(self, *ids):
        """
        Returns the full contents of multiple documents.

        ### Parameters

        - **ids**: the ids of the saved documents.
        """

        return self.redis.execute_command('FT.MGET', self.index_name, *ids)

    def info(self):
        """
        Get info an stats about the the current index, including the number of documents, memory consumption, etc
        """

        res = self.redis.execute_command('FT.INFO', self.index_name)
        it = six.moves.map(to_string, res)
        return dict(six.moves.zip(it, it))

    def _mk_query_args(self, query):
        args = [self.index_name]

        if isinstance(query, six.string_types):
            # convert the query from a text to a query object
            query = Query(query)
        if not isinstance(query, Query):
            raise ValueError("Bad query type %s" % type(query))

        args += query.get_args()
        return args, query

    def search(self, query):
        """
        Search the index for a given query, and return a result of documents

        ### Parameters

        - **query**: the search query. Either a text for simple queries with default parameters, or a Query object for complex queries.
                     See RediSearch's documentation on query format
        """
        args, query = self._mk_query_args(query)
        st = time.time()
        res = self.redis.execute_command(self.SEARCH_CMD, *args)

        return Result(res,
                      not query._no_content,
                      duration=(time.time() - st) * 1000.0,
                      has_payload=query._with_payloads,
                      with_scores=query._with_scores)

    def explain(self, query):
        args, query_text = self._mk_query_args(query)
        return self.redis.execute_command(self.EXPLAIN_CMD, *args)

    def aggregate(self, query):
        """
        Issue an aggregation query

        ### Parameters

        **query**: This can be either an `AggeregateRequest`, or a `Cursor`

        An `AggregateResult` object is returned. You can access the rows from its
        `rows` property, which will always yield the rows of the result
        """
        if isinstance(query, AggregateRequest):
            has_cursor = bool(query._cursor)
            cmd = [self.AGGREGATE_CMD, self.index_name] + query.build_args()
        elif isinstance(query, Cursor):
            has_cursor = True
            cmd = [self.CURSOR_CMD, 'READ',
                   self.index_name] + query.build_args()
        else:
            raise ValueError('Bad query', query)

        raw = self.redis.execute_command(*cmd)
        if has_cursor:
            if isinstance(query, Cursor):
                query.cid = raw[1]
                cursor = query
            else:
                cursor = Cursor(raw[1])
            raw = raw[0]
        else:
            cursor = None

        if isinstance(query, AggregateRequest) and query._with_schema:
            schema = raw[0]
            rows = raw[2:]
        else:
            schema = None
            rows = raw[1:]

        res = AggregateResult(rows, cursor, schema)
        return res

    def spellcheck(self, query, distance=None, include=None, exclude=None):
        """
        Issue a spellcheck query

        ### Parameters

        **query**: search query.
        **distance***: the maximal Levenshtein distance for spelling suggestions (default: 1, max: 4).
        **include**: specifies an inclusion custom dictionary.
        **exclude**: specifies an exclusion custom dictionary.
        """
        cmd = [self.SPELLCHECK_CMD, self.index_name, query]
        if distance:
            cmd.extend(['DISTANCE', distance])

        if include:
            cmd.extend(['TERMS', 'INCLUDE', include])

        if exclude:
            cmd.extend(['TERMS', 'EXCLUDE', exclude])

        raw = self.redis.execute_command(*cmd)

        corrections = {}
        if raw == 0:
            return corrections

        for _correction in raw:
            if isinstance(_correction, six.integer_types) and _correction == 0:
                continue

            if len(_correction) != 3:
                continue
            if not _correction[2]:
                continue
            if not _correction[2][0]:
                continue

            # For spellcheck output
            # 1)  1) "TERM"
            #     2) "{term1}"
            #     3)  1)  1)  "{score1}"
            #             2)  "{suggestion1}"
            #         2)  1)  "{score2}"
            #             2)  "{suggestion2}"
            #
            # Following dictionary will be made
            # corrections = {
            #     '{term1}': [
            #         {'score': '{score1}', 'suggestion': '{suggestion1}'},
            #         {'score': '{score2}', 'suggestion': '{suggestion2}'}
            #     ]
            # }
            corrections[_correction[1]] = [
                {'score': _item[0], 'suggestion':_item[1]}
                for _item in _correction[2]
            ]

        return corrections

    def dict_add(self, name, *terms):
        """Adds terms to a dictionary.

        ### Parameters

        - **name**: Dictionary name.
        - **terms**: List of items for adding to the dictionary.
        """
        cmd = [self.DICT_ADD_CMD, name]
        cmd.extend(terms)
        raw = self.redis.execute_command(*cmd)
        return raw

    def dict_del(self, name, *terms):
        """Deletes terms from a dictionary.

        ### Parameters

        - **name**: Dictionary name.
        - **terms**: List of items for removing from the dictionary.
        """
        cmd = [self.DICT_DEL_CMD, name]
        cmd.extend(terms)
        raw = self.redis.execute_command(*cmd)
        return raw

    def dict_dump(self, name):
        """Dumps all terms in the given dictionary.

        ### Parameters

        - **name**: Dictionary name.
        """
        cmd = [self.DICT_DUMP_CMD, name]
        raw = self.redis.execute_command(*cmd)
        return raw

    def config_set(self, option, value):
        """Set runtime configuration option.

        ### Parameters

        - **option**: the name of the configuration option.
        - **value**: a value for the configuration option.
        """
        cmd = [self.CONFIG_CMD, 'SET', option, value]
        raw = self.redis.execute_command(*cmd)
        return raw == 'OK'

    def config_get(self, option):
        """Get runtime configuration option value.

        ### Parameters

        - **option**: the name of the configuration option.
        """
        cmd = [self.CONFIG_CMD, 'GET', option]
        res = {}
        raw = self.redis.execute_command(*cmd)
        if raw:
            for kvs in raw:
                res[kvs[0]] = kvs[1]
        return res

    def tagvals(self, tagfield):
        """
        Return a list of all possible tag values

        ### Parameters

        - **tagfield**: Tag field name
        """

        cmd = self.redis.execute_command(self.TAGVALS_CMD, self.index_name, tagfield)
        return cmd

    def aliasadd(self, alias):
        """
        Alias a search index - will fail if alias already exists

        ### Parameters

        - **alias**: Name of the alias to create
        """

        cmd = self.redis.execute_command(self.ALIAS_ADD_CMD, alias, self.index_name)
        return cmd

    def aliasupdate(self, alias):
        """
        Updates an alias - will fail if alias does not already exist

        ### Parameters

        - **alias**: Name of the alias to create
        """

        cmd = self.redis.execute_command(self.ALIAS_UPDATE_CMD, alias, self.index_name)
        return cmd

    def aliasdel(self, alias):
        """
        Removes an alias to a search index

        ### Parameters

        - **alias**: Name of the alias to delete
        """

        cmd = self.redis.execute_command(self.ALIAS_DEL_CMD, alias)
        return cmd