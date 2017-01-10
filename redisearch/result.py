import itertools
from .document import Document

class Result(object):
    """
    Represents the result of a search query, and has an array of Document objects
    """

    def __init__(self, res, hascontent, queryText, duration=0, snippet_size = 500):

        self.total = res[0]
        self.duration = duration
        self.docs = []

        tokens = filter(None, queryText.rstrip("\" ").lstrip(" \"").split(' '))
        for i in xrange(1, len(res), 2 if hascontent else 1):
            id = res[i]
            fields = {} 
            if hascontent:
                fields = dict(
                    dict(itertools.izip(res[i + 1][::2], res[i + 1][1::2]))) if hascontent else {}
            try:
                del fields['id']
            except KeyError:
                pass

            doc = Document(id, **fields)
            #print doc
            if hascontent:
                doc.snippetize('body', size=snippet_size, bold_tokens = tokens)
                
            self.docs.append(doc)


    def __repr__(self):

        return 'Result{%d total, docs: %s}' % (self.total, self.docs)
