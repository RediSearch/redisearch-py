import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from rmtest import ModuleTestCase
import redis
import unittest
import random
import time
import bz2
import csv

from redisearch import Client, Document, Result, NumericField, TextField, AutoCompleter, Suggestion

class RedisSearchTestCase(ModuleTestCase('../module.so', fixed_port=6379)):

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
            res = client.search('king', no_content=True)
            self.assertEqual(194, res.total)
            self.assertEqual(10, len(res.docs))
            for doc in res.docs:
                self.assertNotIn('txt', doc.__dict__)
                self.assertNotIn('play', doc.__dict__)
            
            #test verbatim vs no verbatim
            total = client.search('kings', no_content=True).total
            vtotal = client.search('kings', no_content=True, verbatim = True).total
            self.assertGreater(total, vtotal)  

            # test in fields
            txt_total =  client.search('henry', no_content=True, fields = ('txt',)).total
            play_total = client.search('henry', no_content=True, fields = ('play',)).total
            both_total = client.search('henry', no_content=True, fields = ('play','txt')).total
            self.assertEqual(129, txt_total)
            self.assertEqual(494, play_total)
            self.assertEqual(494, both_total)

            # test load_document
            doc = client.load_document('henry vi part 3:62')
            self.assertIsNotNone(doc)
            self.assertEqual('henry vi part 3:62', doc.id)
            self.assertEqual(doc.play, 'Henry VI Part 3')
            self.assertTrue(len(doc.txt) > 0)


    def testExample(self):

        conn = self.redis()

        with conn as r:
            # Creating a client with a given index name
            client = Client('myIndex', port=conn.port)

            # Creating the index definition and schema
            client.create_index((TextField('title', weight=5.0), TextField('body')))

            # Indexing a document
            client.add_document('doc1', title = 'RediSearch', body = 'Redisearch impements a search engine on top of redis')

            # Searching
            res = client.search("search engine")

            

            
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
