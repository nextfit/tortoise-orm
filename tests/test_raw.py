
from tests.testmodels import Event, Tournament
from tortoise.contrib import test


class TestRaw(test.TortoiseTransactionedTestModelsTestCase):
    async def test_raw(self):
        first = await Tournament.create(name="A - First")
        second = await Tournament.create(name="B - Second")
        third = await Tournament.create(name="C - Third")

        db_table = Tournament._meta.db_table

        self.assertEqual(
            list(map(lambda t: t.id, await Tournament.raw(f"select * from {db_table}"))),
            [first.id, second.id, third.id]
        )

    async def test_raw_prefetch(self):
        first = await Tournament.create(name="A - First")
        second = await Tournament.create(name="B - Second")
        third = await Tournament.create(name="C - Third")

        db_table = Tournament._meta.db_table

        await Event.create(name="Aa x Ab", tournament=first)
        await Event.create(name="Ac x Ad", tournament=first)
        await Event.create(name="Ba x Bb", tournament=second)

        t = await Tournament.raw(f"select * from {db_table}").prefetch_related('events')
        self.assertEqual(len(t), 3)

        self.assertEqual(list(map(lambda v: v.name, t[0].events)), ["Aa x Ab", "Ac x Ad"])

        self.assertEqual(list(map(lambda v: v.name, t[1].events)), ["Ba x Bb"])

        self.assertEqual(len(t[2].events), 0)
