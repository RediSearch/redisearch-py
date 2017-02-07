import itertools
from .document import Document

class Result(object):
    """
    Represents the result of a search query, and has an array of Document objects
    """

    def __init__(self, res, hascontent, query_text, duration=0, snippets = None, has_payload = False):
        """
        - **snippets**: An optional dictionary of the form {field: snippet_size} for snippet formatting
        """

        self.total = res[0]
        self.duration = duration
        self.docs = []

        tokens = filter(None, query_text.rstrip("\" ").lstrip(" \"").split(' '))
        step = 1
        if hascontent:
            step = 3 if has_payload else 2
        else:
            # we can't have nocontent and payloads in the same response
            has_payload = False
            
        
        for i in xrange(1, len(res), step):
            id = res[i]
            payload = res[i+1] if has_payload else None
            fields_offset = 2 if has_payload else 1
            
            fields = {} 
            if hascontent:
                fields = dict(
                    dict(itertools.izip(res[i + fields_offset][::2], res[i + fields_offset][1::2]))) if hascontent else {}
            try:
                del fields['id']
            except KeyError:
                pass

            doc = Document(id, payload=payload, **fields)
            #print doc
            if hascontent and snippets:
                for k,v in snippets.iteritems():
                    doc.snippetize(k, size=v, bold_tokens = tokens)
                
            self.docs.append(doc)


    def __repr__(self):

        return 'Result{%d total, docs: %s}' % (self.total, self.docs)
