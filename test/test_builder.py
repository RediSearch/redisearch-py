import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import unittest
import redisearch.aggregation as a
import redisearch.querystring as q
import redisearch.reducers as r

class QueryBuilderTest(unittest.TestCase):
    def testBetween(self):
        b = q.between(1, 10)
        self.assertEqual('[1 10]', str(b))
        b = q.between(None, 10)
        self.assertEqual('[-inf 10]', str(b))
        b = q.between(1, 10, inclusive_min=False)
        self.assertEqual('[(1 10]', str(b))

    def testTags(self):
        self.assertRaises(ValueError, q.tags)
        self.assertEqual('{1 | 2 | 3}', str(q.tags(1,2,3)))
        self.assertEqual('{foo}', str(q.tags('foo')))

    def testUnion(self):
        u = q.union()
        self.assertEqual('', str(u))
        u = q.union(foo='fooval', bar='barval')
        self.assertEqual('(@foo:fooval|@bar:barval)', str(u))
        u = q.union(q.intersect(foo=1, bar=2), q.intersect(foo=3, bar=4))
        self.assertEqual('((@foo:1 @bar:2)|(@foo:3 @bar:4))', str(u))

    def testSpecialNodes(self):
        u = q.union(num=q.between(1, 10))
        self.assertEqual('@num:[1 10]', str(u))
        u = q.union(num=[q.between(1, 10), q.between(100, 200)])
        self.assertEqual('(@num:[1 10]|@num:[100 200])', str(u))
        u = q.union(num=[q.tags('t1', 't2', 't3'), q.tags('t100', 't200', 't300')])
        self.assertEqual('(@num:{t1 | t2 | t3}|@num:{t100 | t200 | t300})', str(u))

    def testGroup(self):
        # Check the group class on its own
        self.assertRaises(ValueError, a.Group, [], [])
        self.assertRaises(ValueError, a.Group, ['foo'], [])
        self.assertRaises(ValueError, a.Group, [], r.count())

        # Single field, single reducer
        g = a.Group('foo', r.count())
        ret = g.build_args()
        self.assertEqual(['GROUPBY', '1', 'foo', 'REDUCE', 'COUNT', '0'], ret)

        # Multiple fields, single reducer
        g = a.Group(['foo', 'bar'], r.count())
        self.assertEqual(['GROUPBY', '2', 'foo', 'bar', 'REDUCE', 'COUNT', '0'],
                         g.build_args())

        # Multiple fields, multiple reducers
        g = a.Group(['foo', 'bar'], [r.count(), r.count_distinct('@fld1')])
        self.assertEqual(['GROUPBY', '2', 'foo', 'bar', 'REDUCE', 'COUNT', '0', 'REDUCE', 'COUNT_DISTINCT', '1', '@fld1'],
                         g.build_args())

    def testAggRequest(self):
        req = a.AggregateRequest()
        self.assertEqual(['*'], req.build_args())

        # Test with group_by
        req = a.AggregateRequest().group_by('@foo', r.count())
        self.assertEqual(['*', 'GROUPBY', '1', '@foo', 'REDUCE', 'COUNT', '0'], req.build_args())

        # Test with group_by and alias on reducer
        req = a.AggregateRequest().group_by('@foo', r.count().alias('foo_count'))
        self.assertEqual(['*', 'GROUPBY', '1', '@foo', 'REDUCE', 'COUNT', '0', 'AS', 'foo_count'], req.build_args())

        # Test with limit
        req = a.AggregateRequest(). \
            group_by('@foo', r.count()). \
            sort_by('@foo')
        self.assertEqual(['*', 'GROUPBY', '1', '@foo', 'REDUCE', 'COUNT', '0', 'SORTBY', '1',
                          '@foo'], req.build_args())

        # Test with apply
        req = a.AggregateRequest(). \
            apply(foo="@bar / 2"). \
            group_by('@foo', r.count())

        self.assertEqual(['*', 'APPLY', '@bar / 2', 'AS', 'foo', 'GROUPBY', '1', '@foo', 'REDUCE', 'COUNT', '0'],
                         req.build_args())

        # Test with filter
        req = a.AggregateRequest().group_by('@foo', r.count()).filter( "@foo=='bar'")
        self.assertEqual(['*', 'GROUPBY', '1', '@foo', 'REDUCE', 'COUNT', '0', 'FILTER', "@foo=='bar'" ], req.build_args())

        # Test with filter on different state of the pipeline
        req = a.AggregateRequest().filter("@foo=='bar'").group_by('@foo', r.count())
        self.assertEqual(['*', 'FILTER', "@foo=='bar'", 'GROUPBY', '1', '@foo','REDUCE', 'COUNT', '0' ], req.build_args())

        # Test with filter on different state of the pipeline
        req = a.AggregateRequest().filter(["@foo=='bar'","@foo2=='bar2'"]).group_by('@foo', r.count())
        self.assertEqual(['*', 'FILTER', "@foo=='bar'", 'FILTER', "@foo2=='bar2'", 'GROUPBY', '1', '@foo', 'REDUCE', 'COUNT', '0'],
                         req.build_args())

        # Test with sort_by
        req = a.AggregateRequest().group_by('@foo', r.count()).sort_by('@date')
        # print req.build_args()
        self.assertEqual(['*', 'GROUPBY', '1', '@foo', 'REDUCE', 'COUNT', '0', 'SORTBY', '1', '@date'],
                         req.build_args())

        req = a.AggregateRequest().group_by('@foo', r.count()).sort_by(a.Desc('@date'))
        # print req.build_args()
        self.assertEqual(['*', 'GROUPBY', '1', '@foo', 'REDUCE', 'COUNT', '0', 'SORTBY', '2', '@date', 'DESC'],
                         req.build_args())

        req = a.AggregateRequest().group_by('@foo', r.count()).sort_by(a.Desc('@date'), a.Asc('@time'))
        # print req.build_args()
        self.assertEqual(['*', 'GROUPBY', '1', '@foo', 'REDUCE', 'COUNT', '0', 'SORTBY', '4', '@date', 'DESC', '@time', 'ASC'],
                         req.build_args())

        req = a.AggregateRequest().group_by('@foo', r.count()).sort_by(a.Desc('@date'), a.Asc('@time'), max=10)
        self.assertEqual(['*', 'GROUPBY', '1', '@foo', 'REDUCE', 'COUNT', '0', 'SORTBY', '4', '@date', 'DESC', '@time', 'ASC', 'MAX', '10'],
                         req.build_args())

    def test_reducers(self):
        self.assertEqual((), r.count().args)
        self.assertEqual(('f1',), r.sum('f1').args)
        self.assertEqual(('f1',), r.min('f1').args)
        self.assertEqual(('f1',), r.max('f1').args)
        self.assertEqual(('f1',), r.avg('f1').args)
        self.assertEqual(('f1',), r.tolist('f1').args)
        self.assertEqual(('f1',), r.count_distinct('f1').args)
        self.assertEqual(('f1',), r.count_distinctish('f1').args)
        self.assertEqual(('f1', '0.95'), r.quantile('f1', 0.95).args)
        self.assertEqual(('f1',), r.stddev('f1').args)

        self.assertEqual(('f1',), r.first_value('f1').args)
        self.assertEqual(('f1', 'BY', 'f2', 'ASC'), r.first_value('f1', a.Asc('f2')).args)
        self.assertEqual(('f1', 'BY', 'f1', 'ASC'), r.first_value('f1', a.Asc).args)

        self.assertEqual(('f1', '50'), r.random_sample('f1', 50).args)

if __name__ == '__main__':

    unittest.main()