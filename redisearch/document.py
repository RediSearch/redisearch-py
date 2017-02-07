class Document(object):
    """
    Represents a single document in a result set 
    """

    def __init__(self, id, payload = None, **fields):

        self.id = id
        self.payload = payload
        for k, v in fields.iteritems():
            setattr(self, k, v)

    def __repr__(self):

        return 'Document %s' % self.__dict__

    def snippetize(self, field, size=500, bold_tokens=tuple()):
        """
        Create a shortened snippet from the document's content 
        :param size: the szie of the snippet in characters. It might be a bit longer or shorter
        :param boldTokens: a list of tokens we want to make bold (basically the query terms)
        """
        txt = getattr(self, field, '')
        for tok in bold_tokens:
            txt = txt.replace(tok, "<b>%s</b>" % tok)
        while size < len(txt) and txt[size] != ' ':
            size+=1

        setattr(self, field, (txt[:size] + '...') if len(txt) > size else txt)
