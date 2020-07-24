
from uuid import UUID, uuid4

from tests.testmodels import UniqueName, UUIDPkModel
from tortoise.contrib import test
from tortoise.exceptions import IntegrityError
from tortoise.transactions import in_transaction


class TestBulk(test.TortoiseTestModelsTestCase):
    async def test_bulk_create_and_update(self):
        await UniqueName.bulk_create([UniqueName() for _ in range(100)])
        all_ = await UniqueName.all()

        self.assertEqual(len(all_), 100)

        first = all_[0].id
        self.assertEqual([{"id": un.id, "name": un.name} for un in all_],
            [{"id": val, "name": None} for val in range(first, first+100)])

        for un in all_:
            un.name = "updated-name-{}".format(un.id)

        await UniqueName.bulk_update(all_, ["name"])

        all_ = await UniqueName.all().order_by("id").values("id", "name")
        first = all_[0]["id"]
        self.assertEqual(all_,
            [{"id": val, "name": "updated-name-{}".format(val)} for val in range(first, first+100)])

    async def test_bulk_create_uuidpk(self):
        await UUIDPkModel.bulk_create([UUIDPkModel() for _ in range(100)])
        res = await UUIDPkModel.all().values_list("id", flat=True)
        self.assertEqual(len(res), 100)
        self.assertIsInstance(res[0], UUID)

    @test.requireCapability(supports_transactions=True)
    async def test_bulk_create_in_transaction(self):
        async with in_transaction():
            await UniqueName.bulk_create([UniqueName() for _ in range(100)])
        all_ = await UniqueName.all().values("id", "name")
        inc = all_[0]["id"]
        self.assertEqual(all_, [{"id": val + inc, "name": None} for val in range(100)])

    @test.requireCapability(supports_transactions=True)
    async def test_bulk_create_uuidpk_in_transaction(self):
        async with in_transaction():
            await UUIDPkModel.bulk_create([UUIDPkModel() for _ in range(100)])
        res = await UUIDPkModel.all().values_list("id", flat=True)
        self.assertEqual(len(res), 100)
        self.assertIsInstance(res[0], UUID)

    async def test_bulk_create_fail(self):
        with self.assertRaises(IntegrityError):
            await UniqueName.bulk_create(
                [UniqueName(name=str(i)) for i in range(10)]
                + [UniqueName(name=str(i)) for i in range(10)]
            )

    async def test_bulk_create_uuidpk_fail(self):
        val = uuid4()
        with self.assertRaises(IntegrityError):
            await UUIDPkModel.bulk_create([UUIDPkModel(id=val) for _ in range(10)])

    @test.requireCapability(supports_transactions=True)
    async def test_bulk_create_in_transaction_fail(self):
        with self.assertRaises(IntegrityError):
            async with in_transaction():
                await UniqueName.bulk_create(
                    [UniqueName(name=str(i)) for i in range(10)]
                    + [UniqueName(name=str(i)) for i in range(10)]
                )

    @test.requireCapability(supports_transactions=True)
    async def test_bulk_create_uuidpk_in_transaction_fail(self):
        val = uuid4()
        with self.assertRaises(IntegrityError):
            async with in_transaction():
                await UUIDPkModel.bulk_create([UUIDPkModel(id=val) for _ in range(10)])
