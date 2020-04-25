from tests import testmodels
from tortoise.contrib import test
from tortoise.exceptions import IntegrityError, NoValuesFetched, OperationalError
from tortoise.query.single import SingleQuerySet


class TestForeignKeyField(test.TestCase):
    async def test_empty(self):
        with self.assertRaises(IntegrityError):
            await testmodels.MinRelation.create()

    async def test_minimal__create_by_id(self):
        tour = await testmodels.Tournament.create(name="Team1")
        rel = await testmodels.MinRelation.create(tournament_id=tour.id)
        self.assertEqual(rel.tournament_id, tour.id)
        self.assertEqual((await tour.minrelation_set.all())[0], rel)

    async def test_minimal__create_by_name(self):
        tour = await testmodels.Tournament.create(name="Team1")
        rel = await testmodels.MinRelation.create(tournament=tour)
        await rel.fetch_related("tournament")
        self.assertEqual(rel.tournament, tour)
        self.assertEqual((await tour.minrelation_set.all())[0], rel)

    async def test_minimal__by_name__created_prefetched(self):
        tour = await testmodels.Tournament.create(name="Team1")
        rel = await testmodels.MinRelation.create(tournament=tour)
        self.assertEqual(rel.tournament, tour)
        self.assertEqual((await tour.minrelation_set.all())[0], rel)

    async def test_minimal__by_name__unfetched(self):
        tour = await testmodels.Tournament.create(name="Team1")
        rel = await testmodels.MinRelation.create(tournament=tour)
        rel = await testmodels.MinRelation.get(id=rel.id)
        self.assertIsInstance(rel.tournament, SingleQuerySet)

    async def test_minimal__by_name__re_awaited(self):
        tour = await testmodels.Tournament.create(name="Team1")
        rel = await testmodels.MinRelation.create(tournament=tour)
        await rel.fetch_related("tournament")
        self.assertEqual(rel.tournament, tour)
        self.assertEqual(await rel.tournament, tour)

    async def test_minimal__by_name__awaited(self):
        tour = await testmodels.Tournament.create(name="Team1")
        rel = await testmodels.MinRelation.create(tournament=tour)
        rel = await testmodels.MinRelation.get(id=rel.id)
        self.assertEqual(await rel.tournament, tour)
        self.assertEqual((await tour.minrelation_set.all())[0], rel)

    async def test_event__create_by_id(self):
        tour = await testmodels.Tournament.create(name="Team1")
        rel = await testmodels.Event.create(name="Event1", tournament_id=tour.id)
        self.assertEqual(rel.tournament_id, tour.id)
        self.assertEqual((await tour.events.all())[0], rel)

    async def test_event__create_by_name(self):
        tour = await testmodels.Tournament.create(name="Team1")
        rel = await testmodels.Event.create(name="Event1", tournament=tour)
        await rel.fetch_related("tournament")
        self.assertEqual(rel.tournament, tour)
        self.assertEqual((await tour.events.all())[0], rel)

    async def test_update_by_name(self):
        tour = await testmodels.Tournament.create(name="Team1")
        tour2 = await testmodels.Tournament.create(name="Team2")
        rel0 = await testmodels.Event.create(name="Event1", tournament=tour)

        await testmodels.Event.filter(id=rel0.id).update(tournament=tour2)
        rel = await testmodels.Event.get(id=rel0.id)

        await rel.fetch_related("tournament")
        self.assertEqual(rel.tournament, tour2)
        self.assertEqual(await tour.events.all(), [])
        self.assertEqual((await tour2.events.all())[0], rel)

    async def test_update_by_id(self):
        tour = await testmodels.Tournament.create(name="Team1")
        tour2 = await testmodels.Tournament.create(name="Team2")
        rel0 = await testmodels.Event.create(name="Event1", tournament_id=tour.id)

        await testmodels.Event.filter(id=rel0.id).update(tournament_id=tour2.id)
        rel = await testmodels.Event.get(id=rel0.id)

        self.assertEqual(rel.tournament_id, tour2.id)
        self.assertEqual(await tour.events.all(), [])
        self.assertEqual((await tour2.events.all())[0], rel)

    async def test_minimal__uninstantiated_create(self):
        tour = testmodels.Tournament(name="Team1")
        with self.assertRaisesRegex(OperationalError, "You should first call .save()"):
            await testmodels.MinRelation.create(tournament=tour)

    async def test_minimal__uninstantiated_iterate(self):
        tour = testmodels.Tournament(name="Team1")
        with self.assertRaisesRegex(
            OperationalError, "This objects hasn't been instanced, call .save()"
        ):
            async for _ in tour.minrelation_set:
                pass

    async def test_minimal__uninstantiated_await(self):
        tour = testmodels.Tournament(name="Team1")
        with self.assertRaisesRegex(
            OperationalError, "This objects hasn't been instanced, call .save()"
        ):
            await tour.minrelation_set

    async def test_minimal__unfetched_contains(self):
        tour = await testmodels.Tournament.create(name="Team1")
        with self.assertRaisesRegex(
            NoValuesFetched,
            "No values were fetched for this relation," " first use .fetch_related()",
        ):
            "a" in tour.minrelation_set  # pylint: disable=W0104

    async def test_minimal__unfetched_iter(self):
        tour = await testmodels.Tournament.create(name="Team1")
        with self.assertRaisesRegex(
            NoValuesFetched,
            "No values were fetched for this relation," " first use .fetch_related()",
        ):
            for _ in tour.minrelation_set:
                pass

    async def test_minimal__unfetched_len(self):
        tour = await testmodels.Tournament.create(name="Team1")
        with self.assertRaisesRegex(
            NoValuesFetched,
            "No values were fetched for this relation," " first use .fetch_related()",
        ):
            len(tour.minrelation_set)

    async def test_minimal__unfetched_bool(self):
        tour = await testmodels.Tournament.create(name="Team1")
        with self.assertRaisesRegex(
            NoValuesFetched,
            "No values were fetched for this relation," " first use .fetch_related()",
        ):
            bool(tour.minrelation_set)

    async def test_minimal__unfetched_getitem(self):
        tour = await testmodels.Tournament.create(name="Team1")
        with self.assertRaisesRegex(
            NoValuesFetched,
            "No values were fetched for this relation," " first use .fetch_related()",
        ):
            tour.minrelation_set[0]  # pylint: disable=W0104

    async def test_minimal__instantiated_create(self):
        tour = await testmodels.Tournament.create(name="Team1")
        await testmodels.MinRelation.create(tournament=tour)

    async def test_minimal__instantiated_iterate(self):
        tour = await testmodels.Tournament.create(name="Team1")
        async for _ in tour.minrelation_set:
            pass

    async def test_minimal__instantiated_await(self):
        tour = await testmodels.Tournament.create(name="Team1")
        await tour.minrelation_set

    async def test_minimal__fetched_contains(self):
        tour = await testmodels.Tournament.create(name="Team1")
        rel = await testmodels.MinRelation.create(tournament=tour)
        await tour.fetch_related("minrelation_set")
        self.assertTrue(rel in tour.minrelation_set)

    async def test_minimal__fetched_iter(self):
        tour = await testmodels.Tournament.create(name="Team1")
        rel = await testmodels.MinRelation.create(tournament=tour)
        await tour.fetch_related("minrelation_set")
        self.assertEqual(list(tour.minrelation_set), [rel])

    async def test_minimal__fetched_len(self):
        tour = await testmodels.Tournament.create(name="Team1")
        await testmodels.MinRelation.create(tournament=tour)
        await tour.fetch_related("minrelation_set")
        self.assertEqual(len(tour.minrelation_set), 1)

    async def test_minimal__fetched_bool(self):
        tour = await testmodels.Tournament.create(name="Team1")
        await tour.fetch_related("minrelation_set")
        self.assertFalse(bool(tour.minrelation_set))
        await testmodels.MinRelation.create(tournament=tour)
        await tour.fetch_related("minrelation_set")
        self.assertTrue(bool(tour.minrelation_set))

    async def test_minimal__fetched_getitem(self):
        tour = await testmodels.Tournament.create(name="Team1")
        rel = await testmodels.MinRelation.create(tournament=tour)
        await tour.fetch_related("minrelation_set")
        self.assertEqual(tour.minrelation_set[0], rel)

        with self.assertRaises(IndexError):
            tour.minrelation_set[1]  # pylint: disable=W0104

    async def test_event__filter(self):
        tour = await testmodels.Tournament.create(name="Team1")
        event1 = await testmodels.Event.create(name="Event1", tournament=tour)
        event2 = await testmodels.Event.create(name="Event2", tournament=tour)
        self.assertEqual(await tour.events.filter(name="Event1"), [event1])
        self.assertEqual(await tour.events.filter(name="Event2"), [event2])
        self.assertEqual(await tour.events.filter(name="Event3"), [])

    async def test_event__all(self):
        tour = await testmodels.Tournament.create(name="Team1")
        event1 = await testmodels.Event.create(name="Event1", tournament=tour)
        event2 = await testmodels.Event.create(name="Event2", tournament=tour)
        self.assertSetEqual(set(await tour.events.all()), {event1, event2})

    async def test_event__order_by(self):
        tour = await testmodels.Tournament.create(name="Team1")
        event1 = await testmodels.Event.create(name="Event1", tournament=tour)
        event2 = await testmodels.Event.create(name="Event2", tournament=tour)
        self.assertEqual(await tour.events.order_by("-name"), [event2, event1])
        self.assertEqual(await tour.events.order_by("name"), [event1, event2])

    async def test_event__limit(self):
        tour = await testmodels.Tournament.create(name="Team1")
        event1 = await testmodels.Event.create(name="Event1", tournament=tour)
        event2 = await testmodels.Event.create(name="Event2", tournament=tour)
        await testmodels.Event.create(name="Event3", tournament=tour)
        self.assertEqual(await tour.events.limit(2).order_by("name"), [event1, event2])

    async def test_event__offset(self):
        tour = await testmodels.Tournament.create(name="Team1")
        await testmodels.Event.create(name="Event1", tournament=tour)
        event2 = await testmodels.Event.create(name="Event2", tournament=tour)
        event3 = await testmodels.Event.create(name="Event3", tournament=tour)
        self.assertEqual(await tour.events.offset(1).order_by("name"), [event2, event3])
