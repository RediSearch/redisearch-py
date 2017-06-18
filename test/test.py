import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from rmtest import ModuleTestCase
import redis
import unittest
import random
import time
import bz2
import csv

from redisearch import *

class RedisSearchTestCase(ModuleTestCase('../module.so')):

    def createIndex(self, client, num_docs = 100):

        assert isinstance(client, Client)
        #conn.flushdb()
        #client = Client('test', port=conn.port)
        client.create_index((TextField('play', weight=5.0), 
                             TextField('txt'), 
                              NumericField('chapter')))
        chapters = {}
        
        with bz2.BZ2File('will_play_text.csv.bz2') as fp:

            r = csv.reader(fp, delimiter=';')
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

        for key, doc in chapters.iteritems():
            
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
                for k in ['inverted_cap_ovh', 'num_docs', 'offsets_per_term_avg', 'fields', 'index_name', 
                            'inverted_cap_mb', 'skip_index_size_mb', 'bytes_per_record_avg', 'inverted_sz_mb', 
                            'num_terms', 'offset_vectors_sz_mb', 'records_per_doc_avg', 'num_records', 
                            'offset_bits_per_record_avg', 'score_index_size_mb']:
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
                
                # test slop and in order
                self.assertEqual(193, client.search(Query('henry king')).total)
                self.assertEqual(2,client.search(Query('henry king').slop(0).in_order()).total)
                self.assertEqual(25,client.search(Query('king henry').slop(0).in_order()).total)
                self.assertEqual(53,client.search(Query('henry king').slop(0)).total)
                self.assertEqual(167,client.search(Query('henry king').slop(100)).total)

    def testPayloads(self):
        
        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('idx', port=conn.port)

            client.create_index((TextField('txt'),))

            client.add_document('doc1', payload = 'foo baz', txt = 'foo bar')
            client.add_document('doc2', txt = 'foo bar')

            q = Query("foo bar").with_payloads()
            res = client.search(q)
            self.assertEqual(2, res.total)
            self.assertEqual('doc1', res.docs[0].id)
            self.assertEqual('foo baz', res.docs[0].payload)
            self.assertEqual('doc2', res.docs[1].id)
            self.assertIsNone(res.docs[1].payload)

    def testReplace(self):
        
        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('idx', port=conn.port)

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
                self.assertEqual('doc1', res2.docs[0].id)
                self.assertEqual('doc2', res2.docs[1].id)

                #print res1, res2

    def testExample(self):

        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('myIndex', port=conn.port)

            # Creating the index definition and schema
            client.create_index((TextField('title', weight=5.0), TextField('body')))

            # Indexing a document
            client.add_document('doc1', title = 'RediSearch', body = 'Redisearch impements a search engine on top of redis')

            # Searching with snippet sizes
            res = client.search("search engine", snippet_sizes = {'body': 50})

            # Searching with complext parameters:
            q = Query("search engine").verbatim().no_content().paging(0,5)

            res = client.search(q)
            

            
        self.assertTrue(True)

    def testAutoComplete(self):
        with self.redis() as r:
            self.assertTrue(True)
            
            ac = AutoCompleter('ac', conn=r)
            n = 0
            with open('titles.csv') as f:
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


                
                



if __name__ == '__main__':

    unittest.main()
