
from tests.testmodels import Product, Brand
from tortoise.contrib import test
from tortoise.query.context import QueryContext
from tortoise.query.expressions import F
from tortoise.query.functions import OuterRef, Subquery
from tortoise.query.ordering import RandomOrdering


class TestFunctions(test.TestCase):

    async def test_random_ordering(self):
        products = Product.all().order_by(RandomOrdering()).limit(20)
        products._make_query(context=QueryContext())
        query_string = products.query.get_sql().replace('`', '"')
        self.assertEqual(query_string,
            'SELECT "id","name","price","brand_id" FROM "store_product" ORDER BY RANDOM() LIMIT 20')

    async def test_ordering_functions(self):
        products = Product.all().order_by((F('id') * 7) % 143).limit(20)
        products._make_query(context=QueryContext())
        query_string = products.query.get_sql().replace('`', '"')
        self.assertEqual(query_string,
            'SELECT "id","name","price","brand_id" FROM "store_product" ORDER BY MOD("id"*7,143) LIMIT 20')

    async def test_annotation(self):
        products = Product.filter(brand_id=OuterRef('id')).limit(1).values_list('name', flat=True)
        brands = Brand.annotate(product_name=Subquery(products))

        brands._make_query(context=QueryContext())
        query_string = brands.query.get_sql().replace('`', '"')

        self.assertEqual(query_string,
            'SELECT "id","name",('
                'SELECT "U1"."name" "0" FROM "store_product" "U1" '
                'WHERE "U1"."brand_id"="store_brand"."id" ORDER BY "U1"."id" ASC LIMIT 1) "product_name" '
            'FROM "store_brand" ORDER BY "id" ASC')

    #
    # Some cases to be considered later
    #

    #
    # async def test_annotation_f(self):
    #     products = Product.all().annotate(new_order=F('id') * 5)
    #     products._make_query(context=QueryContext())
    #
    #     query_string = products.query.get_sql().replace('`', '"')
    #     self.assertEqual(query_string, '')
    #
    # async def test_brands_prefetch_limited_products(self):
    #     subquery = Product.filter(brand=OuterRef('brand')).limit(4).values_list('id', flat=True)
    #     prefetch = Prefetch('products', queryset=Product.filter(id__in=Subquery(subquery)))
    #     brands = Brand.all().prefetch_related(prefetch)
    #
    #     brands._make_query(context=QueryContext())
    #     query_string = brands.query.get_sql().replace('`', '"')
    #     self.assertEqual(query_string, 'SELECT "id","name" FROM "store_brand" ORDER BY "id" ASC')
    #
    # async def test_brands_raw_prefetch_limited_products():
    #     raw_subquery = """
    #         (select "U1"."id" "0"
    #         from "store_product" "U1"
    #         where "U1"."brand_id"={context.top.table}."brand_id"
    #         order by "U1"."id" asc
    #         limit 3)
    #     """
    #
    #     subquery = Product.raw(raw_subquery)
    #     prefetch = Prefetch('products', queryset=Product.filter(id__in=Subquery(subquery)))
    #     brands = Brand.all().prefetch_related(prefetch)
    #
    # async def test_products_prefetch_limit_images():
    #     subquery = Image.filter(product_set=OuterRef('product_set')).limit(4).values_list('id', flat=True)
    #     prefetch = Prefetch('images', queryset=Image.filter(id__in=Subquery(subquery)))
    #     products = Product.all().limit(5).prefetch_related(prefetch)
    #
    #     products._make_query(context=QueryContext())
    #     query_string = products.query.get_sql().replace('`', '"')
    #
