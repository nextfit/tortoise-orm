
from tests.testmodels import Product, Brand
from tortoise.contrib import test
from tortoise.query.context import QueryContext
from tortoise.query.functions import OuterRef, Subquery


class TestOrderBy(test.TestCase):
    async def test_annotation(self):
        products = Product.filter(brand_id=OuterRef('id')).limit(1).values_list('name', flat=True)
        queryset = Brand.annotate(product_name=Subquery(products))
        queryset._make_query(context=QueryContext())

        self.assertEqual(queryset.query, "")
