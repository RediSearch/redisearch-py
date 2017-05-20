from redis import Redis, ConnectionPool

class Suggestion(object):
    """
    Represents a single suggestion being sent or returned from the auto complete server
    """
    def __init__(self, string, score=1.0):

        self.string = string
        self.score = score
    
    def __repr__(self):

        return self.string

class AutoCompleter(object):
    """
    A client to RediSearch's AutoCompleter API

    It provides prefix searches with optionally fuzzy matching of prefixes    
    """
        
    SUGADD_COMMAND = "FT.SUGADD"
    SUGDEL_COMMAND = "FT.SUGDEL"
    SUGLEN_COMMAND = "FT.SUGLEN"
    SUGGET_COMMAND = "FT.SUGGET"

    INCR = 'INCR'
    WITHSCORES = 'WITHSCORES'
    FUZZY = 'FUZZY'

    def __init__(self, key, host='localhost', port=6379, conn = None):
        """
        Create a new AutoCompleter client for the given key, and optional host and port

        If conn is not None, we employ an already existing redis connection
        """

        self.key = key
        self.redis = conn if conn is not None else Redis(
            connection_pool = ConnectionPool(host=host, port=port))

    def add_suggestions(self,  *suggestions, **kwargs):
        """
        Add suggestion terms to the AutoCompleter engine. Each suggestion has a score and string.

        If kwargs['increment'] is true and the terms are already in the server's dictionary, we increment their scores 
        """
        pipe = self.redis.pipeline()
        for sug in suggestions:
            args = [AutoCompleter.SUGADD_COMMAND, self.key, sug.string, sug.score]
            if kwargs.get('increment'):
                args.append(AutoCompleter.INCR)
            
            pipe.execute_command(*args)

        return pipe.execute()[-1]



    def len(self):
        """
        Return the number of entries in the AutoCompleter index
        """
        return self.redis.execute_command(AutoCompleter.SUGLEN_COMMAND, self.key)

    def delete(self, string):
        """
        Delete a string from the AutoCompleter index.
        Returns 1 if the string was found and deleted, 0 otherwise
        """
        return self.redis.execute_command(AutoCompleter.SUGDEL_COMMAND, self.key, string)

    def get_suggestions(self, prefix, fuzzy = False, num = 10, with_scores = False):
        """
        Get a list of suggestions from the AutoCompleter, for a given prefix

        ### Parameters:
        - **prefix**: the prefix we are searching. **Must be valid ascii or utf-8**
        - **fuzzy**: If set to true, the prefix search is done in fuzzy mode. 
            **NOTE**: Running fuzzy searches on short (<3 letters) prefixes can be very slow, and even scan the entire index.
        - **with_scores**: if set to true, we also return the (refactored) score of each suggestion. 
          This is normally not needed, and is NOT the original score inserted into the index
        - **num**: The maximum number of results we return. Note that we might return less. The algorithm trims irrelevant suggestions.
        
        Returns a list of Suggestion objects. If with_scores was False, the score of all suggestions is 1.
        """

        args = [AutoCompleter.SUGGET_COMMAND, self.key, prefix, 'MAX', num]
        if fuzzy:
            args.append(AutoCompleter.FUZZY)
        if with_scores:
            args.append(AutoCompleter.WITHSCORES)
        
        ret = self.redis.execute_command(*args)
        results = []
        if not ret:
            return results

        if with_scores:
            # return suggestiongs with scores
            return [Suggestion(ret[i], float(ret[i+1])) for i in xrange(0, len(ret), 2)]
        
        # return suggestions without scores
        return [Suggestion(s) for s in ret]

        

        
