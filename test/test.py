import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from rmtest import ModuleTestCase
import redis
import unittest
import bz2
import csv
from io import TextIOWrapper

import six

from redisearch import *

WILL_PLAY_TEXT = os.path.abspath(os.path.dirname(__file__)) + '/will_play_text.csv.bz2'

TITLES_CSV = os.path.abspath(os.path.dirname(__file__)) + '/titles.csv'

class RedisSearchTestCase(ModuleTestCase('../module.so')):

    def createIndex(self, client, num_docs = 100):

        assert isinstance(client, Client)
        #conn.flushdb()
        #client = Client('test', port=conn.port)
        try:
            client.create_index((TextField('play', weight=5.0), 
                                TextField('txt'), 
                                NumericField('chapter')))
        except redis.ResponseError:
            client.drop_index()
            return self.createIndex(client, num_docs=num_docs)

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


                res =  client.search("henry iv")
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
        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('idx', port=conn.port)
            try:
                client.drop_index()
            except:
                pass
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
                self.assertEqual('doc2', res2.docs[0].id)
                self.assertEqual('doc1', res2.docs[1].id)

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
                self.assertNotEquals(1.0, ret[0].score)
                self.assertEqual('badalte rishtey', ret[1].string)
                self.assertIsInstance(ret[1].score, float)
                self.assertNotEquals(1.0, ret[1].score)

                ret= ac.get_suggestions('bad', fuzzy=True, num=10)
                self.assertEqual(10, len(ret))
                self.assertEquals(1.0, ret[0].score)
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
        client = Client('idx', port=self.server.port)
        try:
            client.drop_index()
        except:
            pass

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
            # Search for f3 value. All documents should have it
            res = client.search('@f3:f3_val')
            self.assertEqual(2, res.total)

            # Only the document updated with PARTIAL should still have the f1 and f2
            # values
            res = client.search('@f3:f3_val @f2:f2_val @f1:f1_val')
            self.assertEqual(1, res.total)

    def testExplain(self):
        client = self.getCleanClient('idx')
        client.create_index((TextField('f1'), TextField('f2'), TextField('f3')))
        res = client.explain('@f3:f3_val @f2:f2_val @f1:f1_val')
        self.assertTrue(res)

    def testSummarize(self):
        client = self.getCleanClient('idx')
        self.createIndex(client)

        for i in self.retry_with_reload():
            q = Query('king henry').paging(0, 1)
            q.highlight(fields=('play', 'txt'), tags=('<b>', '</b>'))
            q.summarize('txt')

            res = client.search(q)
            doc = res.docs[0]
            self.assertEqual('<b>Henry</b> IV', doc.play)
            self.assertEqual('ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... ',
                            doc.txt)

            q = Query('king henry').paging(0, 1).summarize().highlight()
            doc = client.search(q).docs[0]
            self.assertEqual('<b>Henry</b> ... ', doc.play)
            self.assertEqual('ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... ',
                            doc.txt)

    def testTags(self):
        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('idx', port=conn.port)
            client.redis.flushdb()
            
            client.create_index((TextField('txt'), TagField('tags')))

            client.add_document('doc1', txt = 'fooz barz', tags = 'foo,foo bar,hello;world')
            
            for i in r.retry_with_rdb_reload():

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
            self.assertIn(b'SORTABLE', response['fields'][0])
            self.assertIn(b'NOSTEM', response['fields'][0])

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

if __name__ == '__main__':

    unittest.main()
