from warnings import warn
from .result import Result
from .document import Document
from .client import Client, NumericField, TextField, GeoField, TagField, IndexDefinition
from .query import Query, NumericFilter, GeoFilter, SortbyField
from .aggregation import AggregateRequest, AggregateResult
from .auto_complete import AutoCompleter, Suggestion


warn("Please upgrade to redis-py (https://pypi.org/project/redis/) "
"This library is deprecated, and all features have been merged into redis-py.",
DeprecationWarning, stacklevel=2)
