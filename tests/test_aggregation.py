
from pypika.functions import Count, Min, Sum

from tests.testmodels import Event, Team, Tournament
from tortoise.contrib import test
from tortoise.exceptions import NotARelationFieldError


class TestAggregation(test.TortoiseTransactionedTestModelsTestCase):
    async def test_aggregation(self):
        tournament = Tournament(name="New Tournament")
        await tournament.save()

        await Tournament.create(name="Second tournament")
        await Event(name="Without participants", tournament_id=tournament.id).save()

        event = Event(name="Test", tournament_id=tournament.id)
        await event.save()

        await Team.bulk_create(Team(name="Team {}".format(i + 1)) for i in range(2))
        participants = list(await Team.all())

        await event.participants.add(participants[0], participants[1])
        await event.participants.add(participants[0], participants[1])

        ##############
        tournaments_with_count = (
            await Tournament.all()
            .annotate(events_count=Count("events"))
            .filter(events_count__gte=1)
        )
        self.assertEqual(len(tournaments_with_count), 1)
        self.assertEqual(tournaments_with_count[0].events_count, 2)

        ##############
        event_with_lowest_team_id = (
            await Event.filter(id=event.id).annotate(lowest_team_id=Min("participants__id")).first()
        )
        self.assertEqual(event_with_lowest_team_id.lowest_team_id, participants[0].id)

        ##############
        ordered_tournaments = (
            await Tournament.all().annotate(events_count=Count("events")).order_by("events_count")
        )
        self.assertEqual(len(ordered_tournaments), 2)
        self.assertEqual(ordered_tournaments[1].id, tournament.id)

        ##############
        default_name_tournaments = (
            await Tournament.all().annotate(Count("events")).order_by("events__count")
        )
        self.assertEqual(len(default_name_tournaments), 2)
        self.assertEqual(default_name_tournaments[1].id, tournament.id)

        ##############
        event_with_annotation = (
            await Event.all().annotate(tournament_test_id=Sum("tournament__id")).first()
        )
        self.assertEqual(
            event_with_annotation.tournament_test_id, event_with_annotation.tournament_id
        )

        ##############
        with self.assertRaisesRegex(NotARelationFieldError, str(NotARelationFieldError("name", Event))):
            await Event.all().annotate(tournament_test_id=Sum("name__id")).first()
