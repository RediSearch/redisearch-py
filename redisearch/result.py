from six.moves import xrange, zip as izip

from .document import Document
from ._util import to_string

class Result(object):
    """
    Represents the result of a search query, and has an array of Document objects
    """

    def __init__(self, res, hascontent, duration=0, has_payload = False):
        """
        - **snippets**: An optional dictionary of the form {field: snippet_size} for snippet formatting
        """

        self.total = res[0]
        self.duration = duration
        self.docs = []

        step = 1
        fields_offset = 0
        if hascontent and has_payload:
            step = 3
            fields_offset = 2
        elif hascontent and not has_payload:
            step = 2
            fields_offset = 1
        elif not hascontent and has_payload:
            step = 2
            fields_offset = 0

        for i in xrange(1, len(res), step):
            id = to_string(res[i])
            payload = to_string(res[i+1]) if has_payload else None

            fields = {}
            if fields_offset > 0:
                fields = dict(
                    dict(izip(map(to_string, res[i + fields_offset][::2]),
                              map(to_string, res[i + fields_offset][1::2])))
                )

            try:
                del fields['id']
            except KeyError:
                pass

            doc = Document(id, payload=payload, **fields)
            self.docs.append(doc)

    def __repr__(self):

        return 'Result{%d total, docs: %s}' % (self.total, self.docs)
