#
# from tests.testmodels import Product, Brand
# from tortoise.contrib import test
# from tortoise.query.functions import OuterRef, Subquery
#
#
# class TestFunctions(test.TestCase):
#     async def test_annotation(self):
#         products = Product.filter(brand_id=OuterRef('id')).limit(1).values_list('name', flat=True)
#         queryset = Brand.annotate(product_name=Subquery(products))
#
