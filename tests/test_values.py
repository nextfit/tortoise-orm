from pypika.functions import Length, Trim

from tests.testmodels import Event, Team, Tournament
from tortoise.contrib import test
from tortoise.exceptions import FieldError, UnknownFieldError
from tortoise.query.expressions import F


class TestValues(test.TortoiseTransactionedTestModelsTestCase):
    async def test_values_related_fk(self):
        tournament = await Tournament.create(name="New Tournament")
        await Event.create(name="Test", tournament_id=tournament.id)

        event2 = await Event.filter(name="Test").values("name", "tournament__name")
        self.assertEqual(event2[0], {"name": "Test", "tournament__name": "New Tournament"})

    async def test_values_list_related_fk(self):
        tournament = await Tournament.create(name="New Tournament")
        await Event.create(name="Test", tournament_id=tournament.id)

        event2 = await Event.filter(name="Test").values_list("name", "tournament__name")
        self.assertEqual(event2[0], ("Test", "New Tournament"))

    async def test_values_related_rfk(self):
        tournament = await Tournament.create(name="New Tournament")
        await Event.create(name="Test", tournament_id=tournament.id)

        tournament2 = await Tournament.filter(name="New Tournament").values("name", "events__name")
        self.assertEqual(tournament2[0], {"name": "New Tournament", "events__name": "Test"})

    async def test_values_list_related_rfk(self):
        tournament = await Tournament.create(name="New Tournament")
        await Event.create(name="Test", tournament_id=tournament.id)

        tournament2 = await Tournament.filter(name="New Tournament").values_list(
            "name", "events__name"
        )
        self.assertEqual(tournament2[0], ("New Tournament", "Test"))

    async def test_values_related_m2m(self):
        tournament = await Tournament.create(name="New Tournament")
        event = await Event.create(name="Test", tournament_id=tournament.id)
        team = await Team.create(name="Some Team")
        await event.participants.add(team)

        tournament2 = await Event.filter(name="Test").values("name", "participants__name")
        self.assertEqual(tournament2[0], {"name": "Test", "participants__name": "Some Team"})

    async def test_values_list_related_m2m(self):
        tournament = await Tournament.create(name="New Tournament")
        event = await Event.create(name="Test", tournament_id=tournament.id)
        team = await Team.create(name="Some Team")
        await event.participants.add(team)

        tournament2 = await Event.filter(name="Test").values_list("name", "participants__name")
        self.assertEqual(tournament2[0], ("Test", "Some Team"))

    async def test_values_related_fk_itself(self):
        tournament = await Tournament.create(name="New Tournament")
        await Event.create(name="Test", tournament_id=tournament.id)

        with self.assertRaisesRegex(FieldError, "tournament is a relation. Try a nested field of the related model"):
            await Event.filter(name="Test").values("name", "tournament")

    async def test_values_list_related_fk_itself(self):
        tournament = await Tournament.create(name="New Tournament")
        await Event.create(name="Test", tournament_id=tournament.id)

        with self.assertRaisesRegex(FieldError, "tournament is a relation. Try a nested field of the related model"):
            await Event.filter(name="Test").values_list("name", "tournament")

    async def test_values_related_rfk_itself(self):
        tournament = await Tournament.create(name="New Tournament")
        await Event.create(name="Test", tournament_id=tournament.id)

        with self.assertRaisesRegex(FieldError, "events is a relation. Try a nested field of the related model"):
            await Tournament.filter(name="New Tournament").values("name", "events")

    async def test_values_list_related_rfk_itself(self):
        tournament = await Tournament.create(name="New Tournament")
        await Event.create(name="Test", tournament_id=tournament.id)

        with self.assertRaisesRegex(FieldError, "events is a relation. Try a nested field of the related model"):
            await Tournament.filter(name="New Tournament").values_list("name", "events")

    async def test_values_related_m2m_itself(self):
        tournament = await Tournament.create(name="New Tournament")
        event = await Event.create(name="Test", tournament_id=tournament.id)
        team = await Team.create(name="Some Team")
        await event.participants.add(team)

        with self.assertRaisesRegex(
            FieldError, "participants is a relation. Try a nested field of the related model"
        ):
            await Event.filter(name="Test").values("name", "participants")

    async def test_values_list_related_m2m_itself(self):
        tournament = await Tournament.create(name="New Tournament")
        event = await Event.create(name="Test", tournament_id=tournament.id)
        team = await Team.create(name="Some Team")
        await event.participants.add(team)

        with self.assertRaisesRegex(
            FieldError, "participants is a relation. Try a nested field of the related model"
        ):
            await Event.filter(name="Test").values_list("name", "participants")

    async def test_values_bad_key(self):
        tournament = await Tournament.create(name="New Tournament")
        await Event.create(name="Test", tournament_id=tournament.id)

        with self.assertRaisesRegex(UnknownFieldError, str(UnknownFieldError("neem", Event))):
            await Event.filter(name="Test").values("name", "neem")

    async def test_values_list_bad_key(self):
        tournament = await Tournament.create(name="New Tournament")
        await Event.create(name="Test", tournament_id=tournament.id)

        with self.assertRaisesRegex(UnknownFieldError, str(UnknownFieldError("neem", Event))):
            await Event.filter(name="Test").values_list("name", "neem")

    async def test_values_related_bad_key(self):
        tournament = await Tournament.create(name="New Tournament")
        await Event.create(name="Test", tournament_id=tournament.id)

        with self.assertRaisesRegex(UnknownFieldError, str(UnknownFieldError("neem", Tournament))):
            await Event.filter(name="Test").values("name", "tournament__neem")

    async def test_values_list_related_bad_key(self):
        tournament = await Tournament.create(name="New Tournament")
        await Event.create(name="Test", tournament_id=tournament.id)

        with self.assertRaisesRegex(UnknownFieldError, str(UnknownFieldError("neem", Tournament))):
            await Event.filter(name="Test").values_list("name", "tournament__neem")

    async def test_values_list_annotations_length(self):
        await Tournament.create(name="Championship")
        await Tournament.create(name="Super Bowl")

        tournaments = await Tournament\
            .annotate(name_length=Length("name"))\
            .values_list("name", "name_length")\
            .order_by("name")

        self.assertEqual(tournaments, [("Championship", 12), ("Super Bowl", 10)])

    async def test_values_annotations_length(self):
        await Tournament.create(name="Championship")
        await Tournament.create(name="Super Bowl")

        tournaments = await Tournament\
            .annotate(name_slength=Length("name"))\
            .values("name", "name_slength")\
            .order_by("name")

        self.assertEqual(
            tournaments,
            [
                {"name": "Championship", "name_slength": 12},
                {"name": "Super Bowl", "name_slength": 10},
            ],
        )

    async def test_values_list_annotations_trim(self):
        await Tournament.create(name="  x")
        await Tournament.create(name=" y ")

        tournaments = await Tournament\
            .annotate(name_trim=Trim("name"))\
            .values_list("name", "name_trim")\
            .order_by("id")

        self.assertEqual(tournaments, [("  x", "x"), (" y ", "y")])

    async def test_values_annotations_trim(self):
        await Tournament.create(name="  x")
        await Tournament.create(name=" y ")

        tournaments = await Tournament\
            .annotate(name_trim=Trim("name"))\
            .values("name", "name_trim")\
            .order_by("id")

        self.assertEqual(
            tournaments,
            [
                {"name": "  x", "name_trim": "x"},
                {"name": " y ", "name_trim": "y"}
            ]
        )

    async def test_values_annotations_arithmetic(self):
        await Tournament.create(id=1, name="1")
        await Tournament.create(id=2, name="2")
        await Tournament.create(id=3, name="3")
        await Tournament.create(id=4, name="4")
        await Tournament.create(id=5, name="5")
        await Tournament.create(id=6, name="6")

        tournaments = await Tournament.annotate(new_id=F("id") * 7 + 3).order_by("id").values_list("new_id")
        self.assertEqual(
            tournaments, [(10,), (17,), (24,), (31,), (38,), (45,)]
        )
