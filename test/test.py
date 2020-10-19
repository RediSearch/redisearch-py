import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from rmtest import ModuleTestCase
import redis
import unittest
import bz2
import csv
import time
from io import TextIOWrapper

import six

from redisearch import *
import redisearch.aggregation as aggregations
import redisearch.reducers as reducers

WILL_PLAY_TEXT = os.path.abspath(os.path.dirname(__file__)) + '/will_play_text.csv.bz2'

TITLES_CSV = os.path.abspath(os.path.dirname(__file__)) + '/titles.csv'

def waitForIndex(env, idx, timeout=None):
    delay = 0.1
    while True:
        res = env.execute_command('ft.info', idx)
        try:
            res.index('indexing')
        except:
            break

        if int(res[res.index('indexing') + 1]) == 0:
            break

        time.sleep(delay)
        if timeout is not None:
            timeout -= delay
            if timeout <= 0:
                break

def check_version_2(env):
    try:
        # Indexing the hash
        env.execute_command('FT.ADDHASH foo bar 1')
    except redis.ResponseError as e:
        # Support for FT.ADDHASH was removed in RediSearch 2.0
        if str(e).startswith('unknown command `FT.ADDHASH`'):
            return True
        return False

class RedisSearchTestCase(ModuleTestCase('../module.so')):

    def createIndex(self, client, num_docs = 100, definition=None):

        assert isinstance(client, Client)
        try:
            client.create_index((TextField('play', weight=5.0), 
                                TextField('txt'), 
                                NumericField('chapter')), definition=definition)
        except redis.ResponseError:
            client.drop_index()
            return self.createIndex(client, num_docs=num_docs, definition=definition)

        chapters = {}
        bzfp = bz2.BZ2File(WILL_PLAY_TEXT)
        if six.PY3:
            bzfp = TextIOWrapper(bz2.BZ2File(WILL_PLAY_TEXT), encoding='utf8')

        r = csv.reader(bzfp, delimiter=';')
        for n, line in enumerate(r):
            #['62816', 'Merchant of Venice', '9', '3.2.74', 'PORTIA', "I'll begin it,--Ding, dong, bell."]

            play, chapter, character, text = line[1], line[2], line[4], line[5]

            key = '{}:{}'.format(play, chapter).lower()
            d = chapters.setdefault(key, {})
            d['play'] = play
            d['txt'] = d.get('txt', '') + ' ' + text
            d['chapter'] = int(chapter or 0)
            if len(chapters) == num_docs:
                break

        indexer = client.batch_indexer(chunk_size=50)
        self.assertIsInstance(indexer, Client.BatchIndexer)
        self.assertEqual(50, indexer.chunk_size)

        for key, doc in six.iteritems(chapters):
            indexer.add_document(key, **doc)
        indexer.commit()

    def testClient(self):

        conn = self.redis()

        with conn as r:
            num_docs = 500
            r.flushdb()
            client = Client('test', port=conn.port)

            self.createIndex(client, num_docs =num_docs)

            for _ in r.retry_with_rdb_reload():
                waitForIndex(r, 'test')
                #verify info
                info = client.info()
                for k in [  'index_name', 'index_options', 'fields', 'num_docs',
                            'max_doc_id', 'num_terms', 'num_records', 'inverted_sz_mb',
                            'offset_vectors_sz_mb', 'doc_table_size_mb', 'key_table_size_mb',
                            'records_per_doc_avg', 'bytes_per_record_avg', 'offsets_per_term_avg',
                            'offset_bits_per_record_avg' ]:
                    self.assertIn(k, info)

                self.assertEqual(client.index_name, info['index_name'])
                self.assertEqual(num_docs, int(info['num_docs']))


                res = client.search("henry iv")
                self.assertIsInstance(res, Result)
                assert isinstance(res, Result)
                self.assertEqual(225, res.total)
                self.assertEqual(10, len(res.docs))
                self.assertGreater(res.duration, 0)

                for doc in res.docs:
                    self.assertTrue(doc.id)
                    self.assertEqual(doc.play, 'Henry IV')
                    self.assertTrue(len(doc.txt) > 0)

                # test no content
                res = client.search(Query('king').no_content())
                self.assertEqual(194, res.total)
                self.assertEqual(10, len(res.docs))
                for doc in res.docs:
                    self.assertNotIn('txt', doc.__dict__)
                    self.assertNotIn('play', doc.__dict__)

                #test verbatim vs no verbatim
                total = client.search(Query('kings').no_content()).total
                vtotal = client.search(Query('kings').no_content().verbatim()).total
                self.assertGreater(total, vtotal)

                # test in fields
                txt_total =  client.search(Query('henry').no_content().limit_fields('txt')).total
                play_total = client.search(Query('henry').no_content().limit_fields('play')).total
                both_total = client.search(Query('henry').no_content().limit_fields('play','txt')).total
                self.assertEqual(129, txt_total)
                self.assertEqual(494, play_total)
                self.assertEqual(494, both_total)

                # test load_document
                doc = client.load_document('henry vi part 3:62')
                self.assertIsNotNone(doc)
                self.assertEqual('henry vi part 3:62', doc.id)
                self.assertEqual(doc.play, 'Henry VI Part 3')
                self.assertTrue(len(doc.txt) > 0)

                # test inkeys
                ids = [x.id for x in client.search(Query('henry')).docs]
                self.assertEqual(10, len(ids))
                subset = ids[:5]
                docs = client.search(Query('henry').limit_ids(*subset))
                self.assertEqual(len(subset), docs.total)
                ids = [x.id for x in docs.docs]
                self.assertEqual(set(ids), set(subset))
 
#                 self.assertRaises(redis.ResponseError, client.search, Query('henry king').return_fields('play', 'nonexist'))

                # test slop and in order
                self.assertEqual(193, client.search(Query('henry king')).total)
                self.assertEqual(3,client.search(Query('henry king').slop(0).in_order()).total)
                self.assertEqual(52,client.search(Query('king henry').slop(0).in_order()).total)
                self.assertEqual(53,client.search(Query('henry king').slop(0)).total)
                self.assertEqual(167,client.search(Query('henry king').slop(100)).total)

                # test delete document
                client.add_document('doc-5ghs2', play = 'Death of a Salesman')
                res = client.search(Query('death of a salesman'))
                self.assertEqual(1, res.total)

                self.assertEqual(1, client.delete_document('doc-5ghs2'))
                res = client.search(Query('death of a salesman'))
                self.assertEqual(0, res.total)
                self.assertEqual(0, client.delete_document('doc-5ghs2'))

                client.add_document('doc-5ghs2', play = 'Death of a Salesman')
                res = client.search(Query('death of a salesman'))
                self.assertEqual(1, res.total)
                client.delete_document('doc-5ghs2')

    def getCleanClient(self, name):
        """
        Gets a client client attached to an index name which is ready to be
        created
        """
        client = Client(name, port=self.server.port)
        try:
            client.drop_index()
        except:
            pass

        return client

    def testAddHash(self):
        conn = self.redis()

        with conn as r:
            if check_version_2(r):
                return
            # Creating a client with a given index name
            client = Client('idx', port=conn.port)

            client.redis.flushdb()
            # Creating the index definition and schema
            client.create_index((TextField('title', weight=5.0), TextField('body')))

            client.redis.hset(
                'doc1',
                mapping={
                    'title': 'RediSearch',
                    'body': 'Redisearch impements a search engine on top of redis'
                })

            client.add_document_hash('doc1')

            # Searching with complext parameters:
            q = Query("search engine").verbatim().no_content().paging(0, 5)
            res = client.search(q)
            self.assertEqual('doc1', res.docs[0].id)

    def testPayloads(self):
        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('idx', port=conn.port)
            client.redis.flushdb()
            client.create_index((TextField('txt'),))

            client.add_document('doc1', payload = 'foo baz', txt = 'foo bar')
            client.add_document('doc2', txt = 'foo bar')

            q = Query("foo bar").with_payloads()
            res = client.search(q)
            self.assertEqual(2, res.total)
            self.assertEqual('doc2', res.docs[0].id)

            self.assertEqual('doc1', res.docs[1].id)
            self.assertEqual('foo baz', res.docs[1].payload)
            self.assertIsNone(res.docs[0].payload)

    def testScores(self):
        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('idx', port=conn.port)
            client.redis.flushdb()
            client.create_index((TextField('txt'),))

            client.add_document('doc1', txt = 'foo baz')
            client.add_document('doc2', txt = 'foo bar')

            q = Query("foo ~bar").with_scores()
            res = client.search(q)
            self.assertEqual(2, res.total)

            self.assertEqual('doc2', res.docs[0].id)
            self.assertEqual(3.0, res.docs[0].score)

            self.assertEqual('doc1', res.docs[1].id)
            # todo: enable once new RS version is tagged
            #self.assertEqual(0.2, res.docs[1].score)

    def testReplace(self):
        
        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('idx', port=conn.port)
            client.redis.flushdb()
            client.create_index((TextField('txt'),))

            client.add_document('doc1', txt = 'foo bar')
            client.add_document('doc2', txt = 'foo bar')

            res = client.search("foo bar")
            self.assertEqual(2, res.total)
            client.add_document('doc1', replace = True, txt = 'this is a replaced doc')

            res = client.search("foo bar")
            self.assertEqual(1, res.total)
            self.assertEqual('doc2', res.docs[0].id)

            res = client.search("replaced doc")
            self.assertEqual(1, res.total)
            self.assertEqual('doc1', res.docs[0].id)

    def testStopwords(self): 
        # Creating a client with a given index name
        client = self.getCleanClient('idx')

        client.create_index((TextField('txt'),), stopwords = ['foo', 'bar', 'baz'])
        client.add_document('doc1', txt = 'foo bar')
        client.add_document('doc2', txt = 'hello world')

        q1 = Query("foo bar").no_content()
        q2 = Query("foo bar hello world").no_content()
        res1, res2 =  client.search(q1), client.search(q2)
        self.assertEqual(0, res1.total)
        self.assertEqual(1, res2.total)

    def testFilters(self):
        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('idx', port=conn.port)
            client.redis.flushdb()

            client.create_index((TextField('txt'), NumericField('num'), GeoField('loc')))
            client.add_document('doc1', txt = 'foo bar', num = 3.141, loc = '-0.441,51.458')
            client.add_document('doc2', txt = 'foo baz', num = 2, loc = '-0.1,51.2')

            for i in r.retry_with_rdb_reload():
                waitForIndex(r, 'idx')
                # Test numerical filter     
                q1 = Query("foo").add_filter(NumericFilter('num', 0, 2)).no_content()
                q2 = Query("foo").add_filter(NumericFilter('num', 2, NumericFilter.INF, minExclusive=True)).no_content()
                res1, res2 =  client.search(q1), client.search(q2)

                self.assertEqual(1, res1.total)
                self.assertEqual(1, res2.total)
                self.assertEqual('doc2', res1.docs[0].id)
                self.assertEqual('doc1', res2.docs[0].id)

                # Test geo filter
                q1 = Query("foo").add_filter(GeoFilter('loc', -0.44, 51.45, 10)).no_content()
                q2 = Query("foo").add_filter(GeoFilter('loc', -0.44, 51.45, 100)).no_content()
                res1, res2 =  client.search(q1), client.search(q2)
                
                self.assertEqual(1, res1.total)
                self.assertEqual(2, res2.total)
                self.assertEqual('doc1', res1.docs[0].id)
                
                # Sort results, after RDB reload order may change
                list = [res2.docs[0].id, res2.docs[1].id]
                list.sort()
                self.assertEqual(['doc1', 'doc2'], list)

    def testPayloadsWithNoContent(self):
        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('idx', port=conn.port)
            client.redis.flushdb()
            client.create_index((TextField('txt'),))

            client.add_document('doc1', payload = 'foo baz', txt = 'foo bar')
            client.add_document('doc2', payload = 'foo baz2', txt = 'foo bar')

            q = Query("foo bar").with_payloads().no_content()
            res = client.search(q)
            self.assertEqual(2, len(res.docs))

    def testSortby(self):
        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('idx', port=conn.port)
            client.redis.flushdb()
            
            client.create_index((TextField('txt'), NumericField('num', sortable=True)))
            client.add_document('doc1', txt = 'foo bar', num = 1)
            client.add_document('doc2', txt = 'foo baz', num = 2)
            client.add_document('doc3', txt = 'foo qux', num = 3)

            # Test sort
            q1 = Query("foo").sort_by('num', asc=True).no_content()
            q2 = Query("foo").sort_by('num', asc=False).no_content()
            res1, res2 = client.search(q1), client.search(q2)
            
            self.assertEqual(3, res1.total)
            self.assertEqual('doc1', res1.docs[0].id)
            self.assertEqual('doc2', res1.docs[1].id)
            self.assertEqual('doc3', res1.docs[2].id)
            self.assertEqual(3, res2.total)
            self.assertEqual('doc1', res2.docs[2].id)
            self.assertEqual('doc2', res2.docs[1].id)
            self.assertEqual('doc3', res2.docs[0].id)

    def testExample(self):
        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('myIndex', port=conn.port)
            client.redis.flushdb()
            
            # Creating the index definition and schema
            client.create_index((TextField('title', weight=5.0), TextField('body')))

            # Indexing a document
            client.add_document('doc1', title = 'RediSearch', body = 'Redisearch impements a search engine on top of redis')

            # Searching with complext parameters:
            q = Query("search engine").verbatim().no_content().paging(0,5)

            res = client.search(q)
        self.assertTrue(True)

    def testAutoComplete(self):
        with self.redis() as r:
            self.assertTrue(True)

            ac = AutoCompleter('ac', conn=r)
            n = 0
            with open(TITLES_CSV) as f:
                cr = csv.reader(f)

                for row in cr:
                    n+=1
                    term, score = row[0], float(row[1])
                    #print term, score
                    self.assertEqual(n,ac.add_suggestions(Suggestion(term,score=score)))

            self.assertEqual(n, ac.len())
            strs = []
            for _ in r.retry_with_rdb_reload():
                ret = ac.get_suggestions('bad', with_scores = True)
                self.assertEqual(2, len(ret))
                self.assertEqual('badger', ret[0].string)
                self.assertIsInstance(ret[0].score, float)
                self.assertNotEqual(1.0, ret[0].score)
                self.assertEqual('badalte rishtey', ret[1].string)
                self.assertIsInstance(ret[1].score, float)
                self.assertNotEqual(1.0, ret[1].score)

                ret= ac.get_suggestions('bad', fuzzy=True, num=10)
                self.assertEqual(10, len(ret))
                self.assertEqual(1.0, ret[0].score)
                strs = {x.string for x in ret}

            for sug in strs:
                self.assertEqual(1, ac.delete(sug))
            # make sure a second delete returns 0
            for sug in strs:
                self.assertEqual(0, ac.delete(sug))

            # make sure they were actually deleted
            ret2 = ac.get_suggestions('bad', fuzzy=True, num=10)
            for sug in ret2:
                self.assertNotIn(sug.string, strs)

            # Test with payload
            ac.add_suggestions(Suggestion('pay1', payload='pl1'))
            ac.add_suggestions(Suggestion('pay2', payload='pl2'))
            ac.add_suggestions(Suggestion('pay3', payload='pl3'))

            sugs = ac.get_suggestions('pay', with_payloads=True, with_scores=True)
            self.assertEqual(3, len(sugs))
            for sug in sugs:
                self.assertTrue(sug.payload)
                self.assertTrue(sug.payload.startswith('pl'))

    def testNoIndex(self):
        # Creating a client with a given index name
        client = self.getCleanClient('idx')

        client.create_index(
            (TextField('f1', no_index=True, sortable=True), TextField('f2')))

        client.add_document('doc1', f1='MarkZZ', f2='MarkZZ')
        client.add_document('doc2', f1='MarkAA', f2='MarkAA')

        res = client.search(Query('@f1:Mark*'))
        self.assertEqual(0, res.total)

        res = client.search(Query('@f2:Mark*'))
        self.assertEqual(2, res.total)

        res = client.search(Query('@f2:Mark*').sort_by('f1', asc=False))
        self.assertEqual(2, res.total)
        self.assertEqual('doc1', res.docs[0].id)

        res = client.search(Query('@f2:Mark*').sort_by('f1', asc=True))
        self.assertEqual('doc2', res.docs[0].id)

        # Ensure exception is raised for non-indexable, non-sortable fields
        self.assertRaises(Exception, TextField,
                          'name', no_index=True, sortable=False)

    def testPartial(self):
        client = self.getCleanClient('idx')
        client.create_index((TextField('f1'), TextField('f2'), TextField('f3')))

        client.add_document('doc1', f1='f1_val', f2='f2_val')
        client.add_document('doc2', f1='f1_val', f2='f2_val')

        client.add_document('doc1', f3='f3_val', partial=True)
        client.add_document('doc2', f3='f3_val', replace=True)

        for i in self.retry_with_reload():
            waitForIndex(client.redis, 'idx')
            # Search for f3 value. All documents should have it
            res = client.search('@f3:f3_val')
            self.assertEqual(2, res.total)

            # Only the document updated with PARTIAL should still have the f1 and f2
            # values
            res = client.search('@f3:f3_val @f2:f2_val @f1:f1_val')
            self.assertEqual(1, res.total)

    def testNoCreate(self):
        client = self.getCleanClient('idx')
        client.create_index((TextField('f1'), TextField('f2'), TextField('f3')))

        client.add_document('doc1', f1='f1_val', f2='f2_val')
        client.add_document('doc2', f1='f1_val', f2='f2_val')

        client.add_document('doc1', f3='f3_val', no_create=True)
        client.add_document('doc2', f3='f3_val', no_create=True, partial=True)

        for i in self.retry_with_reload():
            waitForIndex(client.redis, 'idx')
            # Search for f3 value. All documents should have it
            res = client.search('@f3:f3_val')
            self.assertEqual(2, res.total)

            # Only the document updated with PARTIAL should still have the f1 and f2
            # values
            res = client.search('@f3:f3_val @f2:f2_val @f1:f1_val')
            self.assertEqual(1, res.total)
            
        with self.assertRaises(redis.ResponseError) as error:
            client.add_document('doc3', f2='f2_val', f3='f3_val', no_create=True)

    def testExplain(self):
        client = self.getCleanClient('idx')
        client.create_index((TextField('f1'), TextField('f2'), TextField('f3')))
        res = client.explain('@f3:f3_val @f2:f2_val @f1:f1_val')
        self.assertTrue(res)

    def testSummarize(self):
        client = self.getCleanClient('idx')
        self.createIndex(client)

        for i in self.retry_with_reload():
            waitForIndex(client.redis, 'idx')
            q = Query('king henry').paging(0, 1)
            q.highlight(fields=('play', 'txt'), tags=('<b>', '</b>'))
            q.summarize('txt')

            doc = sorted(client.search(q).docs)[0]
            self.assertEqual('<b>Henry</b> IV', doc.play)
            self.assertEqual('ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... ',
                            doc.txt)

            q = Query('king henry').paging(0, 1).summarize().highlight()
            
            doc = sorted(client.search(q).docs)[0]
            self.assertEqual('<b>Henry</b> ... ', doc.play)
            self.assertEqual('ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... ',
                            doc.txt)

    def testAlias(self):
        conn = self.redis()
        with conn as r:
            if check_version_2(r):

                index1 = Client('testAlias', port=conn.port)
                index1.redis.flushdb()
                index2 = Client('testAlias2', port=conn.port)

                index1.redis.hset("index1:lonestar", mapping = {'name': 'lonestar'})
                index2.redis.hset("index2:yogurt", mapping = {'name': 'yogurt'})

                time.sleep(2)

                def1 =IndexDefinition(prefix=['index1:'],score_field='name')
                def2 =IndexDefinition(prefix=['index2:'],score_field='name')
                
                index1.create_index((TextField('name'),),definition=def1)
                index2.create_index((TextField('name'),),definition=def2)

                res = index1.search('*').docs[0]
                self.assertEqual('index1:lonestar', res.id)

                # create alias and check for results
                index1.aliasadd("spaceballs")
                alias_client = Client('spaceballs', port=conn.port)
                res = alias_client.search('*').docs[0]
                self.assertEqual('index1:lonestar', res.id)

                # We should throw an exception when trying to add an alias that already exists
                with self.assertRaises(Exception) as context:
                    index2.aliasadd('spaceballs')
                self.assertEqual('Alias already exists', str(context.exception))

                #update alias and ensure new results
                index2.aliasupdate("spaceballs")
                alias_client2 = Client('spaceballs', port=conn.port)
                res = alias_client2.search('*').docs[0]
                self.assertEqual('index2:yogurt', res.id)

                index2.aliasdel("spaceballs")
                with self.assertRaises(Exception) as context:
                    alias_client2.search('*').docs[0]
                self.assertEqual('spaceballs: no such index', str(context.exception))
                
            else:

                # Creating a client with one index
                index1 = Client('testAlias', port=conn.port)
                index1.redis.flushdb()

                index1.create_index((TextField('txt'),))
                index1.add_document('doc1', txt = 'text goes here')

                index2 = Client('testAlias2', port=conn.port)
                index2.create_index((TextField('txt'),))
                index2.add_document('doc2', txt = 'text goes here')


                # add the actual alias and check
                index1.aliasadd('myalias')
                alias_client = Client('myalias', port=conn.port)
                res = alias_client.search('*').docs[0]
                self.assertEqual('doc1', res.id)

                # We should throw an exception when trying to add an alias that already exists
                with self.assertRaises(Exception) as context:
                    index2.aliasadd('myalias')
                self.assertEqual('Alias already exists', str(context.exception))

                # update the alias and ensure we get doc2
                index2.aliasupdate('myalias')
                alias_client2 = Client('myalias', port=conn.port)
                res = alias_client2.search('*').docs[0]
                self.assertEqual('doc2', res.id)

                # delete the alias and expect an error if we try to query again
                index2.aliasdel('myalias')
                with self.assertRaises(Exception) as context:
                    alias_client2.search('*').docs[0]
                self.assertEqual('myalias: no such index', str(context.exception))

    def testTags(self):
        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('idx', port=conn.port)
            client.redis.flushdb()

            client.create_index((TextField('txt'), TagField('tags')))

            tags  = 'foo,foo bar,hello;world'
            tags2 = 'soba,ramen'

            client.add_document('doc1', txt = 'fooz barz', tags = tags)
            client.add_document('doc2', txt = 'noodles', tags = tags2)

            for i in r.retry_with_rdb_reload():
                waitForIndex(r, 'idx')
                q = Query("@tags:{foo}")
                res = client.search(q)
                self.assertEqual(1, res.total)

                q = Query("@tags:{foo bar}")
                res = client.search(q)
                self.assertEqual(1, res.total)

                q = Query("@tags:{foo\\ bar}")
                res = client.search(q)
                self.assertEqual(1, res.total)

                q = Query("@tags:{hello\\;world}")
                res = client.search(q)
                self.assertEqual(1, res.total)

                q2 = client.tagvals('tags')
                self.assertEqual((tags.split(',') + tags2.split(',')).sort(), q2.sort())

    def testTextFieldSortableNostem(self):
        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('sortableNostem', port=conn.port)
            client.redis.flushdb()

            # Creating the index definition with sortable and no_stem
            client.create_index((TextField('txt', sortable=True, no_stem=True),))

            # Now get the index info to confirm its contents
            response = client.info()
            self.assertIn('SORTABLE', response['fields'][0])
            self.assertIn('NOSTEM', response['fields'][0])

    def testAlterSchemaAdd(self):
        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('alterIdx', port=conn.port)
            client.redis.flushdb()

            # Creating the index definition and schema
            client.create_index((TextField('title'),))

            # Using alter to add a field
            client.alter_schema_add((TextField('body'),))

            # Indexing a document
            client.add_document('doc1', title = 'MyTitle', body = 'Some content only in the body')

            # Searching with parameter only in the body (the added field)
            q = Query("only in the body")

            # Ensure we find the result searching on the added body field
            res = client.search(q)
            self.assertEqual(1, res.total)

    def testSpellCheck(self):
        client = self.getCleanClient('idx')
        client.create_index((TextField('f1'), TextField('f2')))

        client.add_document('doc1', f1='some valid content', f2='this is sample text')
        client.add_document('doc2', f1='very important', f2='lorem ipsum')

        for i in self.retry_with_reload():
            waitForIndex(client.redis, 'idx')
            res = client.spellcheck('impornant')
            self.assertEqual('important', res['impornant'][0]['suggestion'])

            res = client.spellcheck('contnt')
            self.assertEqual('content', res['contnt'][0]['suggestion'])

    def testDictOps(self):
        client = self.getCleanClient('idx')
        client.create_index((TextField('f1'), TextField('f2')))

        for i in self.retry_with_reload():
            waitForIndex(client.redis, 'idx')
            # Add three items
            res = client.dict_add('custom_dict', 'item1', 'item2', 'item3')
            self.assertEqual(3, res)

            # Remove one item
            res = client.dict_del('custom_dict', 'item2')
            self.assertEqual(1, res)

            # Dump dict and inspect content
            res = client.dict_dump('custom_dict')
            self.assertEqual(['item1', 'item3'], res)

            # Remove rest of the items before reload
            client.dict_del('custom_dict', *res)

    def testPhoneticMatcher(self):
        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('myIndex', port=conn.port)
            client.redis.flushdb()

            client.create_index((TextField('name'),))

            client.add_document('doc1', name='Jon')
            client.add_document('doc2', name='John')

            res = client.search(Query("Jon"))
            self.assertEqual(1, len(res.docs))
            self.assertEqual('Jon', res.docs[0].name)

            # Drop and create index with phonetic matcher
            client.redis.flushdb()

            client.create_index((TextField('name', phonetic_matcher='dm:en'),))

            client.add_document('doc1', name='Jon')
            client.add_document('doc2', name='John')

            res = client.search(Query("Jon"))
            self.assertEqual(2, len(res.docs))
            self.assertEqual(['John', 'Jon'], sorted([d.name for d in res.docs]))

    def testGet(self):
        client = self.getCleanClient('idx')
        client.create_index((TextField('f1'), TextField('f2')))

        self.assertEqual([None], client.get('doc1'))
        self.assertEqual([None, None], client.get('doc2', 'doc1'))

        client.add_document('doc1', f1='some valid content dd1', f2='this is sample text ff1')
        client.add_document('doc2', f1='some valid content dd2', f2='this is sample text ff2')

        self.assertEqual([['f1', 'some valid content dd2', 'f2', 'this is sample text ff2']], client.get('doc2'))
        self.assertEqual([['f1', 'some valid content dd1', 'f2', 'this is sample text ff1'], ['f1', 'some valid content dd2', 'f2', 'this is sample text ff2']], client.get('doc1', 'doc2'))

    def testConfig(self):
        client = self.getCleanClient('idx')
        self.assertTrue(client.config_set('TIMEOUT', '100'))
        self.assertFalse(client.config_set('TIMEOUT', "null"))
        res = client.config_get('*')
        self.assertEqual('100', res['TIMEOUT'])
        res = client.config_get('TIMEOUT')
        self.assertEqual('100', res['TIMEOUT'])

    def testAggregations(self):
        conn = self.redis()

        with conn as r:
            client = Client('myIndex', port=conn.port)
            client.redis.flushdb()

            # Creating the index definition and schema
            client.create_index((NumericField('random_num'), TextField('title'),
                                TextField('body'), TextField('parent')))

            # Indexing a document
            client.add_document(
                'search',
                title='RediSearch',
                body='Redisearch impements a search engine on top of redis',
                parent='redis',
                random_num=10)
            client.add_document(
                'ai',
                title='RedisAI',
                body=
                'RedisAI executes Deep Learning/Machine Learning models and managing their data.',
                parent='redis',
                random_num=3)
            client.add_document(
                'json',
                title='RedisJson',
                body=
                'RedisJSON implements ECMA-404 The JSON Data Interchange Standard as a native data type.',
                parent='redis',
                random_num=8)

            req = aggregations.AggregateRequest('redis').group_by(
                "@parent",
                reducers.count(),
                reducers.count_distinct('@title'),
                reducers.count_distinctish('@title'),
                reducers.sum("@random_num"),
                reducers.min("@random_num"),
                reducers.max("@random_num"),
                reducers.avg("@random_num"),
                reducers.stddev("random_num"),
                reducers.quantile("@random_num", 0.5),
                reducers.tolist("@title"),
                reducers.first_value("@title"),
                reducers.random_sample("@title", 2),
            )

            res = client.aggregate(req)

            res = res.rows[0]

            self.assertEqual(len(res), 26)
            self.assertEqual('redis', res[1])
            self.assertEqual('3', res[3])
            self.assertEqual('3', res[5])
            self.assertEqual('3', res[7])
            self.assertEqual('21', res[9])
            self.assertEqual('3', res[11])
            self.assertEqual('10', res[13])
            self.assertEqual('7', res[15])
            self.assertEqual('3.60555127546', res[17])
            self.assertEqual('10', res[19])
            self.assertEqual(['RediSearch', 'RedisAI', 'RedisJson'], res[21])
            self.assertEqual('RediSearch', res[23])
            self.assertEqual(2, len(res[25]))

    def testIndexDefiniontion(self):
        conn = self.redis()

        with conn as r:
            r.flushdb()
            if not check_version_2(r):
                return
            client = Client('test', port=conn.port)

            definition = IndexDefinition(prefix=['hset:', 'henry'],
            filter='@f1==32', language='English', language_field='play',
            score_field='chapter', score=0.5, payload_field='txt' )

            self.assertEqual(['ON','HASH', 'PREFIX',2,'hset:','henry',
            'FILTER','@f1==32','LANGUAGE_FIELD','play','LANGUAGE','English',
            'SCORE_FIELD','chapter','SCORE',0.5,'PAYLOAD_FIELD','txt'],
            definition.args)

            self.createIndex(client, num_docs=500, definition=definition)


    def testCreateClientDefiniontion(self):
        conn = self.redis()

        with conn as r:
            r.flushdb()
            if not check_version_2(r):
                return
            client = Client('test', port=conn.port)

            definition = IndexDefinition(prefix=['hset:', 'henry'])
            self.createIndex(client, num_docs=500, definition=definition)

            info = client.info()
            self.assertEqual(494, int(info['num_docs']))

            r.hset('hset:1', 'f1', 'v1');

            info = client.info()
            self.assertEqual(495, int(info['num_docs']))


if __name__ == '__main__':
    unittest.main()
