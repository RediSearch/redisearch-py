
class Query(object):
    """
    Query is used to build complex queries that have more parameters than just the query string.
    The query string is set in the constructor, and other options have setter functions.

    The setter functions return the query object, so they can be chained, 
    i.e. `Query("foo").verbatim().filter(...)` etc.
    """

    def __init__(self, query_string):
        """
        Create a new query object. 
        The query string is set in the constructor, and other options have setter functions.
        """

        self._query_string = query_string
        self._offset = 0
        self._num = 10
        self._no_content = False
        self._no_stopwords = False
        self._fields = None
        self._verbatim = False
        self._with_payloads = False
        self._filters = list()
        self._ids = None
        self._slop = -1
        self._in_order = False
        self._sortby = None
        self._return_fields = []
        self._summarize_fields = []
        self._highlight_fields = []

    def query_string(self):
        """
        Return the query string of this query only
        """
        return self._query_string

    def limit_ids(self, *ids):
        """
        Limit the results to a specific set of pre-known document ids of any length
        """
        self._ids = ids
        return self

    def return_fields(self, *fields):
        """
        Only return values from these fields
        """
        self._return_fields = fields
        return self

    def summarize(self, *fields, **options):
        """
        Return an abridged format of the field, containing only the segments of
        the field which contain the matching term(s).

        If `fields` is specified, then only the mentioned fields are
        summarized; otherwise all results are summarized.

        Options are `context_len`, `num_frags`, and `sep`

        You can use `hlsum` or `highlight` if you wish to also format the match
        itself distinctly.
        """
        args = ['SUMMARIZE']
        if fields:
            args += ['FIELDS', str(len(fields))]
            args.append(*fields)

        if 'context_len' in options:
            args += ['LEN', str(options['context_len'])]
        if 'num_frags' in options:
            args += ['FRAGS', str(options['num_frags'])]
        if 'sep' in options:
            args += ['SEPARATOR', options['sep']]

        self._summarize_fields = args
        return self

    def highlight(self, *fields, **options):
        """
        Apply specified markup to matched term(s) within the returned field(s)

        """
        args = ['HIGHLIGHT']
        if fields:
            args += ['FIELDS', str(len(fields))] + list(fields)
        if 'tags' in options:
            args += ['TAGS', options['tags'][0], options['tags'][1]]

        self._highlight_fields = args
        return self

    def slop(self, slop):
        """
        Allow a masimum of N intervening non matched terms between phrase terms (0 means exact phrase)
        """
        self._slop = slop
        return self

    def in_order(self):
        """
        Match only documents where the query terms appear in the same order in the document.
        i.e. for the query 'hello world', we do not match 'world hello'
        """
        self._in_order = True
        return self

    def get_args(self):
        """
        Format the redis arguments for this query and return them
        """

        args = [self._query_string]

        if self._no_content:
            args.append('NOCONTENT')

        if self._fields:

            args.append('INFIELDS')
            args.append(len(self._fields))
            args += self._fields
        
        if self._verbatim:
            args.append('VERBATIM')

        if self._no_stopwords:
            args.append('NOSTOPWORDS')

        if self._filters:
            for flt in self._filters:
                assert isinstance(flt, Filter)
                args += flt.args

        if self._with_payloads:
            args.append('WITHPAYLOADS')
        
        if self._ids:
            args.append('INKEYS')
            args.append(len(self._ids))
            args += self._ids

        if self._slop >= 0:
            args += ['SLOP', self._slop]

        if self._in_order:
            args.append('INORDER')

        if self._return_fields:
            args.append('RETURN')
            args.append(len(self._return_fields))
            args += self._return_fields

        if self._sortby:
            assert isinstance(self._sortby, SortbyField)
            args.append('SORTBY')
            args += self._sortby.args

        args += self._summarize_fields + self._highlight_fields
        args += ["LIMIT", self._offset, self._num]
        return args

    def paging(self, offset, num):
        """
        Set the paging for the query (defaults to 0..10).

        - **offset**: Paging offset for the results. Defaults to 0
        - **num**: How many results do we want
        """
        self._offset = offset
        self._num = num
        return self

    def verbatim(self):
        """
        Set the query to be verbatim, i.e. use no query expansion or stemming
        """
        self._verbatim = True
        return self

    def no_content(self):
        """
        Set the query to only return ids and not the document content
        """
        self._no_content = True
        return self

    def no_stopwords(self):
        """
        Prevent the query from being filtered for stopwords. 
        Only useful in very big queries that you are certain contain no stopwords.
        """
        self._no_stopwords = True
        return self

    def with_payloads(self):
        """
        Ask the engine to return document payloads
        """
        self._with_payloads = True
        return self
    
    def limit_fields(self, *fields):
        """
        Limit the search to specific TEXT fields only

        - **fields**: A list of strings, case sensitive field names from the defined schema
        """
        self._fields = fields
        return self

    def add_filter(self, flt):
        """
        Add a numeric or geo filter to the query. 
        **Currently only one of each filter is supported by the engine**

        - **flt**: A NumericFilter or GeoFilter object, used on a corresponding field
        """

        self._filters.append(flt)
        return self

    def sort_by(self, field, asc=True):
        """
        Add a sortby field to the query

        - **field** - the name of the field to sort by
        - **asc** - when `True`, sorting will be done in asceding order
        """
        self._sortby = SortbyField(field, asc)
        return self


class Filter(object):

    def __init__(self, keyword, field, *args):

        self.args = [keyword, field] + list(args)
        
class NumericFilter(Filter):

    INF = '+inf'
    NEG_INF = '-inf'

    def __init__(self, field, minval, maxval, minExclusive = False, maxExclusive = False):

        args = [
            minval if not minExclusive else '({}'.format(minval),
            maxval if not maxExclusive else '({}'.format(maxval),
        ]

        Filter.__init__(self, 'FILTER', field, *args)

class GeoFilter(Filter):

    METERS = 'm'
    KILOMETERS = 'km'
    FEET = 'ft'
    MILES = 'mi'

    def __init__(self, field, lon, lat, radius, unit = KILOMETERS):

        Filter.__init__(self, 'GEOFILTER', field, lon, lat, radius, unit)

class SortbyField(object):

    def __init__(self, field, asc=True):

        self.args = [field, 'ASC' if asc else 'DESC']