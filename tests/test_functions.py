import asyncio

from tests.testmodels import Product, Brand, Image
from tortoise.contrib import test
from tortoise.query import Prefetch
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

    async def test_annotation_f(self):
        products = Product.all().annotate(new_order=F('id') * 5)
        products._make_query(context=QueryContext())

        query_string = products.query.get_sql().replace('`', '"')
        self.assertEqual(query_string,
            'SELECT "id","name","price","brand_id","id"*5 "new_order" FROM "store_product" ORDER BY "id" ASC')

    async def create_objects(self):
        brands = [Brand(name='brand_{}'.format(num)) for num in range(1, 7)]
        await asyncio.gather(*[b.save() for b in brands])

        products = [Product(name='product_{}'.format(num), price='$1') for num in range(1, 22)]

        brand_k, counter = 0, 0
        for p in products:
            p.brand = brands[brand_k]
            counter += 1
            if counter >= brand_k + 1:
                brand_k += 1
                counter = 0

        await asyncio.gather(*[p.save() for p in products])

        # brand_    1  2  2  3  3  3  4  4  4   4   5   5   5   5   5   6   6   6   6   6   6
        # product_  1  2  3  4  5  6  7  8  9  10  11  12  13  14  15  16  17  18  19  20  21

        images = [Image(src='image_{}'.format(num)) for num in range(1, 22)]
        await asyncio.gather(*[img.save() for img in images])

        index, count = 0, 1
        for p in products[5::-1]:
            await p.images.add(*images[index:index+count])
            index, count = index+count, count+1

        # image_    1  2  3  4  5  6  7  8  9  10  11  12  13  14  15  16  17  18  19  20  21
        # product_  6  5  5  4  4  4  3  3  3   3   2   2   2   2   2   1   1   1   1   1   1

    async def test_brands_prefetch_limited_products(self):
        if Product._meta.db.capabilities.dialect == "mysql":
            raise test.SkipTest("This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'")

        await self.create_objects()

        subquery = Product.filter(brand=OuterRef('brand')).limit(3).values_list('id', flat=True)
        prefetch = Prefetch('products', queryset=Product.filter(id__in=Subquery(subquery)))
        brands_fetched = await Brand.all().prefetch_related(prefetch)
        brands_distilled = [{'name': b.name, 'products': [p.name for p in b.products]} for b in brands_fetched]

        self.assertEqual(brands_distilled, [
            {'name': 'brand_1', 'products': ['product_1']},
            {'name': 'brand_2', 'products': ['product_2', 'product_3']},
            {'name': 'brand_3', 'products': ['product_4', 'product_5', 'product_6']},
            {'name': 'brand_4', 'products': ['product_7', 'product_8', 'product_9']},
            {'name': 'brand_5', 'products': ['product_11', 'product_12', 'product_13']},
            {'name': 'brand_6', 'products': ['product_16', 'product_17', 'product_18']},
        ])

    async def test_brands_raw_prefetch_limited_products(self):
        if Product._meta.db.capabilities.dialect == "mysql":
            raise test.SkipTest("This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'")

        await self.create_objects()

        raw_subquery = """
            (select "U1"."id" "0"
            from "store_product" "U1"
            where "U1"."brand_id"={context.top.table}."brand_id"
            order by "U1"."id" asc
            limit 2)
        """

        subquery = Product.raw(raw_subquery)
        prefetch = Prefetch('products', queryset=Product.filter(id__in=Subquery(subquery)))
        brands_fetched = await Brand.all().prefetch_related(prefetch)
        brands_distilled = [{'name': b.name, 'products': [p.name for p in b.products]} for b in brands_fetched]

        self.assertEqual(brands_distilled, [
            {'name': 'brand_1', 'products': ['product_1']},
            {'name': 'brand_2', 'products': ['product_2', 'product_3']},
            {'name': 'brand_3', 'products': ['product_4', 'product_5']},
            {'name': 'brand_4', 'products': ['product_7', 'product_8']},
            {'name': 'brand_5', 'products': ['product_11', 'product_12']},
            {'name': 'brand_6', 'products': ['product_16', 'product_17']},
        ])

    async def test_products_prefetch_limit_images(self):
        if Product._meta.db.capabilities.dialect == "mysql":
            raise test.SkipTest("This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'")

        await self.create_objects()

        subquery = Image.filter(product_set=OuterRef('product_set')).limit(4).values_list('id', flat=True)
        prefetch = Prefetch('images', queryset=Image.filter(id__in=Subquery(subquery)))
        products_fetched = await Product.all().limit(5).prefetch_related(prefetch)
        products_distilled = [{'name': p.name, 'images': [img.src for img in p.images]} for p in products_fetched]

        self.assertEqual(products_distilled, [
            {'name': 'product_1', 'images': ['image_16', 'image_17', 'image_18', 'image_19']},
            {'name': 'product_2', 'images': ['image_11', 'image_12', 'image_13', 'image_14']},
            {'name': 'product_3', 'images': ['image_7', 'image_8', 'image_9', 'image_10']},
            {'name': 'product_4', 'images': ['image_4', 'image_5', 'image_6']},
            {'name': 'product_5', 'images': ['image_2', 'image_3']},
        ])

