
from tests.testmodels import Product, Brand
from tortoise.contrib import test
from tortoise.query.context import QueryContext
from tortoise.query.functions import OuterRef, Subquery


class TestFunctions(test.TestCase):
    async def test_annotation(self):
        products = Product.filter(brand_id=OuterRef('id')).limit(1).values_list('name', flat=True)
        brands = Brand.annotate(product_name=Subquery(products))

        brands._make_query(context=QueryContext())
        query_string = str(brands.query).replace('`', '"')

        self.assertEqual(query_string,
            'SELECT "id","name",('
                'SELECT "U1"."name" "0" FROM "store_product" "U1" '
                'WHERE "U1"."brand_id"="store_brand"."id" ORDER BY "U1"."id" ASC LIMIT 1) "product_name" '
            'FROM "store_brand" ORDER BY "id" ASC')
