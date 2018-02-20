from unittest import TestCase
import redisearch.aggregation as a
import redisearch.querystring as q
import redisearch.reducers as r

class QueryBuilderTest(TestCase):
    def testBetween(self):
        b = q.between(1, 10)
        self.assertEqual('[1 10]', b.to_string())
        b = q.between(None, 10)
        self.assertEqual('[-inf 10]', b.to_string())
        b = q.between(1, 10, inclusive_min=False)
        self.assertEqual('[(1 10]', b.to_string())

    def testTags(self):
        self.assertRaises(ValueError, q.tags)
        self.assertEqual('{1 | 2 | 3}', q.tags(1,2,3).to_string())
        self.assertEqual('{foo}', q.tags('foo').to_string())

    def testUnion(self):
        u = q.union()
        self.assertEqual('', u.to_string())
        u = q.union(foo='fooval', bar='barval')
        self.assertEqual('(@foo:fooval|@bar:barval)', u.to_string())
        u = q.union(q.intersect(foo=1, bar=2), q.intersect(foo=3, bar=4))
        self.assertEqual('((@foo:1 @bar:2)|(@foo:3 @bar:4))', u.to_string())

    def testSpecialNodes(self):
        u = q.union(num=q.between(1, 10))
        self.assertEqual('@num:[1 10]', u.to_string())
        u = q.union(num=[q.between(1, 10), q.between(100, 200)])
        self.assertEqual('(@num:[1 10]|@num:[100 200])', u.to_string())
        u = q.union(num=[q.tags('t1', 't2', 't3'), q.tags('t100', 't200', 't300')])
        self.assertEqual('(@num:{t1 | t2 | t3}|@num:{t100 | t200 | t300})', u.to_string())
