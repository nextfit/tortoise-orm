from tests import testmodels
from tortoise.contrib import test
from tortoise.exceptions import ConfigurationError, IntegrityError
from tortoise.fields import JSONField


class TestJSONFields(test.TortoiseTransactionedTestModelsTestCase):
    async def test_empty(self):
        with self.assertRaises(IntegrityError):
            await testmodels.JSONFields.create()

    async def test_create(self):
        obj0 = await testmodels.JSONFields.create(data={"some": ["text", 3]})
        obj = await testmodels.JSONFields.get(id=obj0.id)
        self.assertEqual(obj.data, {"some": ["text", 3]})
        self.assertEqual(obj.data_null, None)
        await obj.save()
        obj2 = await testmodels.JSONFields.get(id=obj.id)
        self.assertEqual(obj, obj2)

    async def test_update(self):
        obj0 = await testmodels.JSONFields.create(data={"some": ["text", 3]})
        await testmodels.JSONFields.filter(id=obj0.id).update(data={"other": ["text", 5]})
        obj = await testmodels.JSONFields.get(id=obj0.id)
        self.assertEqual(obj.data, {"other": ["text", 5]})
        self.assertEqual(obj.data_null, None)

    async def test_list(self):
        obj0 = await testmodels.JSONFields.create(data=["text", 3])
        obj = await testmodels.JSONFields.get(id=obj0.id)
        self.assertEqual(obj.data, ["text", 3])
        self.assertEqual(obj.data_null, None)
        await obj.save()
        obj2 = await testmodels.JSONFields.get(id=obj.id)
        self.assertEqual(obj, obj2)

    async def test_values(self):
        obj0 = await testmodels.JSONFields.create(data={"some": ["text", 3]})
        values = await testmodels.JSONFields.filter(id=obj0.id).values("data")
        self.assertEqual(values[0]["data"], {"some": ["text", 3]})

    @test.requireCapability(dialect="postgres")
    async def test_values_deep(self):
        await testmodels.JSONFields.create(data={
            "product": {
                "name": "product_1",
                "brand": {
                    "name": "brand_11"
                },
                "images": [
                    {
                        "name": "image_11"
                    },
                    {
                        "name": "image_12"
                    }
                ]
            }
        })

        await testmodels.JSONFields.create(data={
            "product": {
                "name": "product_2",
                "brand": {
                    "name": "brand_21"
                },
                "images": [
                    {
                        "name": "image_21"
                    },
                    {
                        "name": "image_22"
                    }
                ]
            }
        })

        values = await testmodels.JSONFields.all().values("data__product__brand__name")
        self.assertEqual(values, [
            {"data__product__brand__name": "brand_11"},
            {"data__product__brand__name": "brand_21"}
        ])

        values = await testmodels.JSONFields.all().values("data__product__images__0__name")
        self.assertEqual(values, [
            {"data__product__images__0__name": "image_11"},
            {"data__product__images__0__name": "image_21"}
        ])

    async def test_values_list(self):
        obj0 = await testmodels.JSONFields.create(data={"some": ["text", 3]})
        values = await testmodels.JSONFields.filter(id=obj0.id).values_list("data", flat=True)
        self.assertEqual(values[0], {"some": ["text", 3]})

    def test_unique_fail(self):
        with self.assertRaisesRegex(ConfigurationError, "can't be indexed"):
            JSONField(unique=True)

    def test_index_fail(self):
        with self.assertRaisesRegex(ConfigurationError, "can't be indexed"):
            JSONField(db_index=True)

    @test.requireCapability(dialect="postgres")
    async def test_filter(self):
        from tests.testmodels import JSONFields
        await JSONFields.create(data={"customer": "John Doe", "items": {"product": "Beer", "qty": 6}})
        await JSONFields.bulk_create(objects=[
            JSONFields(data={"customer": "Lily Bush", "items": {"product": "Diaper","qty": 24}}),
            JSONFields(data={"customer": "Josh William", "items": {"product": "Toy Car","qty": 1}}),
            JSONFields(data={"customer": "Mary Clark", "items": {"product": "Toy Train","qty": 2}})
        ])

        values = [v.data async for v in JSONFields.filter(data__customer="Lily Bush")]
        self.assertEqual(values, [{"customer": "Lily Bush", "items": {"product": "Diaper","qty": 24}}])

        values = [v.data async for v in JSONFields.filter(data__items__product="Beer")]
        self.assertEqual(values, [{"customer": "John Doe", "items": {"product": "Beer", "qty": 6}}])

        values = [v.data async for v in JSONFields.filter(data__items__qty__gt=7)]
        self.assertEqual(values, [{"customer": "Lily Bush", "items": {"product": "Diaper","qty": 24}}])
