import itertools
from .document import Document

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
        if hascontent:
            step = 3 if has_payload else 2
        else:
            # we can't have nocontent and payloads in the same response
            has_payload = False

        for i in range(1, len(res), step):
            id = res[i].decode('utf-8')
            payload = res[i+1] if has_payload else None
            fields_offset = 2 if has_payload else 1

            fields = {}
            if hascontent:
                fields = {k.decode('utf-8'):v.decode('utf-8') \
                        for k,v in zip(res[i + fields_offset][::2], res[i + fields_offset][1::2])} if hascontent else {}
            try:
                del fields['id']
            except KeyError:
                pass

            doc = Document(id, payload=payload, **fields)
            self.docs.append(doc)

    def __repr__(self):

        return 'Result{%d total, docs: %s}' % (self.total, self.docs)
