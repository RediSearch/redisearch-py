from warnings import warn
from .result import Result
from .document import Document
from .client import Client, NumericField, TextField, GeoField, TagField, IndexDefinition
from .query import Query, NumericFilter, GeoFilter, SortbyField
from .aggregation import AggregateRequest, AggregateResult
from .auto_complete import AutoCompleter, Suggestion


warn("As of redis-py 4.0.0 this library is deprecated. "
"It's features have been merged into redis-py. "
"Please either install it from pypi or the repo."
, DeprecationWarning, stacklevel=2)
