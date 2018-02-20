FIELDNAME = object()


class Reducer(object):
    """
    Base reducer object for all reducers.

    See the `redisearch.reducers` module for the actual reducers.
    """
    NAME = None

    def __init__(self, *args):
        self._args = args
        self._field = None
        self._alias = None
        pass

    def alias(self, alias):
        """
        Set the alias for this reducer.

        ### Parameters

        - **alias**: The value of the alias for this reducer. If this is the
            special value `aggregation.FIELDNAME` then this reducer will be
            aliased using the same name as the field upon which it operates.
            Note that using `FIELDNAME` is only possible on reducers which
            operate on a single field value.

        This method returns the `Reducer` object making it suitable for
        chaining.
        """
        if alias is FIELDNAME:
            if not self._field:
                raise ValueError("Cannot use FIELDNAME alias with no field")
            # Chop off initial '@'
            alias = self._field[1:]
        self._alias = alias
        return self

    @property
    def args(self):
        return self._args


class SortDirection(object):
    """
    This special class is used to indicate sort direction.
    """
    DIRSTRING = None

    def __init__(self, field):
        self.field = field


class Asc(SortDirection):
    """
    Indicate that the given field should be sorted in ascending order
    """
    DIRSTRING = 'ASC'


class Desc(SortDirection):
    """
    Indicate that the given field should be sorted in descending order
    """
    DIRSTRING = 'DESC'


class Group(object):
    """
    This object automatically created in the `AggregateRequest.group_by()`
    """
    def __init__(self, *fields):
        self.fields = fields
        self.reducers = []
        self.limit = [0, 0]

    def add_reducer(self, reducer):
        self.reducers.append(reducer)

    def validate(self):
        if not self.reducers:
            raise ValueError('Need at least one reducer')

    def build_args(self):
        self.validate()
        if not self.fields:
            raise Exception('No fields to group by')
        ret = [str(len(self.fields))]
        ret.extend(self.fields)
        for reducer in self.reducers:
            ret += ['REDUCE', reducer.NAME, str(len(reducer.args))]
            ret.extend(reducer.args)
            if reducer._alias:
                ret += ['AS', reducer._alias]
        return ret


class AggregateRequest(object):
    """
    Aggregation request which can be passed to `Client.aggregate`.
    """
    def __init__(self, query='*'):
        """
        Create an aggregation request. This request may then be passed to
        `client.aggregate()`.

        In order for the request to be usable, it must contain at least one
        group.

        - **query** Query string for filtering records.

        All member methods (except `build_args()`)
        return the object itself, making them useful for chaining.
        """
        self._query = query
        self._groups = []
        self._projections = []
        self._loadfields = []
        self._limit = []
        self._sortby = []
        self._max = 0

    def load(self, *fields):
        """
        Indicate the fields to be returned in the response. These fields are
        returned in addition to any others implicitly specified.

        ### Parameters

        - **fields**: One or more fields in the format of `@field`
        """
        self._loadfields.extend(fields)
        return self

    def group_by(self, fields, *reducers):
        """
        Specify by which fields to group the aggregation.

        ### Parameters

        - **fields**: Fields to group by. This can either be a single string,
            or a list of strings. both cases, the field should be specified as
            `@field`.
        - **reducers**: One or more reducers. Reducers may be found in the
            `aggregation` module.
        """
        if isinstance(fields, basestring):
            fields = [fields]
        group = Group(*fields)

        if not reducers:
            raise ValueError("Must pass at least one reducer")

        for reducer in reducers:
            group.add_reducer(reducer)

        self._groups.append(group)

        return self

    def apply(self, **kwexpr):
        """
        Specify one or more projection expressions to add to each result

        ### Parameters

        - **kwexpr**: One or more key-value pairs for a projection. The key is
            the alias for the projection, and the value is the projection
            expression itself, for example `apply(square_root="sqrt(@foo)")`
        """
        for alias, expr in kwexpr.items():
            self._projections.append([alias, expr])

        return self

    def limit(self, offset, num):
        """
        Sets the limit for the most recent group or query.

        If no group has been defined yet (via `group_by()`) then this sets
        the limit for the initial pool of results from the query. Otherwise,
        this limits the number of items operated on from the previous group.

        Setting a limit on the initial search results may be useful when
        attempting to execute an aggregation on a sample of a large data set.

        ### Parameters

        - **offset**: Result offset from which to begin paging
        - **num**: Number of results to return


        Example of sorting the initial results:

        ```
        AggregateRequest('@sale_amount:[10000, inf]')\
            .limit(0, 10)\
            .group_by('@state', r.count())
        ```

        Will only group by the states found in the first 10 results of the
        query `@sale_amount:[10000, inf]`. On the other hand,

        ```
        AggregateRequest('@sale_amount:[10000, inf]')\
            .limit(0, 1000)\
            .group_by('@state', r.count()\
            .limit(0, 10)
        ```

        Will group all the results matching the query, but only return the
        first 10 groups.

        If you only wish to return a *top-N* style query, consider using
        `sort_by()` instead.

        """
        if self._groups:
            self._groups[-1].limit = [offset, num]
        else:
            self._limit = [offset, num]
        return self

    def sort_by(self, fields, max=0):
        """
        Indicate how the results should be sorted. This can also be used for
        *top-N* style queries

        ### Parameters

        - **fields**: The fields by which to sort. This can be either a single
            field or a list of fields. If you wish to specify order, you can
            use the `Asc` or `Desc` wrapper classes.
        - **max**: Maximum number of results to return. This can be used instead
            of `LIMIT` and is also faster.


        Example of sorting by `foo` ascending and `bar` descending:

        ```
        sort_by(Asc('@foo'), Desc('@bar'))
        ```

        Return the top 10 customers:

        ```
        AggregateRequest()\
            .group_by('@customer', r.sum('@paid').alias(FIELDNAME))\
            .sort_by(Desc('@paid'), max=10)
        ```
        """
        self._max = max
        if isinstance(fields, (basestring, SortDirection)):
            fields = [fields]
        for f in fields:
            if isinstance(f, SortDirection):
                self._sortby += [f.field, f.DIRSTRING]
            else:
                self._sortby.append(f)
        return self

    def validate(self):
        if not self._groups:
            raise ValueError('Request requires at least one group')

    def build_args(self):
        self.validate()
        # @foo:bar ...
        ret = [self._query]
        if self._loadfields:
            ret.append('LOAD')
            ret.append(str(len(self._loadfields)))
            ret.extend(self._loadfields)
        for group in self._groups:
            ret += ['GROUPBY']
            ret.extend(group.build_args())
            if group.limit:
                ret += ['LIMIT'] + [str(x) for x in group.limit]
        for alias, projector in self._projections:
            ret += ['APPLY', projector]
            if alias:
                ret += ['AS', alias]

        if self._sortby:
            ret += ['SORTBY', str(len(self._sortby))]
            ret += self._sortby
            if self._max:
                ret += ['MAX', str(self._max)]

        if self._limit:
            ret += ['LIMIT'] + [str(x) for x in self._limit]

        return ret


