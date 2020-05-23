from pypika.functions import Count

from tests.testmodels import Address, Event, Team, Tournament
from tests.testmodels.store import create_store_objects, Product
from tortoise.contrib import test
from tortoise.exceptions import FieldError, UnknownFieldError
from tortoise.query import Prefetch


class TestPrefetching(test.TestCase):
    async def test_prefetch(self):
        tournament = await Tournament.create(name="tournament")
        event = await Event.create(name="First", tournament=tournament)
        await Event.create(name="Second", tournament=tournament)
        team = await Team.create(name="1")
        team_second = await Team.create(name="2")
        await event.participants.add(team, team_second)
        tournament = await Tournament.all().prefetch_related("events__participants").first()
        self.assertEqual(len(tournament.events[0].participants), 2)
        self.assertEqual(len(tournament.events[1].participants), 0)

    async def test_prefetch_object(self):
        tournament = await Tournament.create(name="tournament")
        await Event.create(name="First", tournament=tournament)
        await Event.create(name="Second", tournament=tournament)
        tournament_with_filtered = (
            await Tournament.all()
            .prefetch_related(Prefetch("events", queryset=Event.filter(name="First")))
            .first()
        )
        tournament = await Tournament.all().prefetch_related("events").first()
        self.assertEqual(len(tournament_with_filtered.events), 1)
        self.assertEqual(len(tournament.events), 2)

    async def test_prefetch_unknown_field(self):
        with self.assertRaises(FieldError):
            tournament = await Tournament.create(name="tournament")
            await Event.create(name="First", tournament=tournament)
            await Event.create(name="Second", tournament=tournament)
            await Tournament.all().prefetch_related(
                Prefetch("events1", queryset=Event.filter(name="First"))
            ).first()

    async def test_prefetch_m2m(self):
        tournament = await Tournament.create(name="tournament")
        event = await Event.create(name="First", tournament=tournament)
        team = await Team.create(name="1")
        team_second = await Team.create(name="2")
        await event.participants.add(team, team_second)
        fetched_events = (
            await Event.all()
            .prefetch_related(Prefetch("participants", queryset=Team.filter(name="1")))
            .first()
        )
        self.assertEqual(len(fetched_events.participants), 1)

    async def test_prefetch_o2o(self):
        tournament = await Tournament.create(name="tournament")
        event = await Event.create(name="First", tournament=tournament)
        await Address.create(city="Santa Monica", street="Ocean", event=event)

        fetched_events = await Event.all().prefetch_related("address").first()

        self.assertEqual(fetched_events.address.city, "Santa Monica")

    async def test_prefetch_nested(self):
        tournament = await Tournament.create(name="tournament")
        event = await Event.create(name="First", tournament=tournament)
        await Event.create(name="Second", tournament=tournament)
        team = await Team.create(name="1")
        team_second = await Team.create(name="2")
        await event.participants.add(team, team_second)
        fetched_tournaments = (
            await Tournament.all()
            .prefetch_related(
                Prefetch("events", queryset=Event.filter(name="First")),
                Prefetch("events__participants", queryset=Team.filter(name="1")),
            )
            .first()
        )
        self.assertEqual(len(fetched_tournaments.events[0].participants), 1)

    async def test_prefetch_nested_with_aggregation(self):
        tournament = await Tournament.create(name="tournament")
        event = await Event.create(name="First", tournament=tournament)
        await Event.create(name="Second", tournament=tournament)
        team = await Team.create(name="1")
        team_second = await Team.create(name="2")
        await event.participants.add(team, team_second)
        fetched_tournaments = (
            await Tournament.all()
            .prefetch_related(
                Prefetch(
                    "events", queryset=Event.annotate(teams=Count("participants")).filter(teams=2)
                )
            )
            .first()
        )
        self.assertEqual(len(fetched_tournaments.events), 1)
        self.assertEqual(fetched_tournaments.events[0].id, event.id)

    async def test_prefetch_direct_relation(self):
        tournament = await Tournament.create(name="tournament")
        await Event.create(name="First", tournament=tournament)
        event = await Event.all().prefetch_related("tournament").first()
        self.assertEqual(event.tournament.id, tournament.id)

    async def test_prefetch_bad_key(self):
        tournament = await Tournament.create(name="tournament")
        await Event.create(name="First", tournament=tournament)
        with self.assertRaisesRegex(UnknownFieldError, str(UnknownFieldError("tour1nament", Event))):
            await Event.all().prefetch_related("tour1nament").first()

    async def test_prefetch_m2m_filter(self):
        tournament = await Tournament.create(name="tournament")
        team = await Team.create(name="1")
        team_second = await Team.create(name="2")
        event = await Event.create(name="First", tournament=tournament)
        await event.participants.add(team, team_second)
        event = await Event.all().prefetch_related(
            Prefetch("participants", Team.filter(name="2"))
        ).first()

        self.assertEqual(len(event.participants), 1)
        self.assertEqual(list(event.participants), [team_second])

    async def test_select_related(self):
        tournament = await Tournament.create(name="tournament")
        await Event.create(name="First", tournament=tournament)

        event = await Event.all().select_related("tournament").first()
        self.assertEqual(event.tournament.id, tournament.id)

    async def test_store_select_related(self):
        await create_store_objects()

        products = await Product.all().select_related('brand', 'brand__image', 'vendor').limit(7)
        products_distilled = [
            {
                'name': p.name,
                'brand': {
                    'name': p.brand.name,
                    'image': {
                        'src': p.brand.image.src
                    }
                },
                'vendor': {
                    'name': p.vendor.name,
                }
            }
            for p in products
        ]

        self.assertEqual(products_distilled, [
            {
                'name': 'product_1',
                'brand': {'name': 'brand_1', 'image': {'src': 'brand_image_1'}},
                'vendor': {'name': 'vendor_1'},
            },
            {
                'name': 'product_2',
                'brand': {'name': 'brand_2', 'image': {'src': 'brand_image_2'}},
                'vendor': {'name': 'vendor_2'},
            },
            {
                'name': 'product_3',
                'brand': {'name': 'brand_2', 'image': {'src': 'brand_image_2'}},
                'vendor': {'name': 'vendor_3'},
            },
            {
                'name': 'product_4',
                'brand': {'name': 'brand_3', 'image': {'src': 'brand_image_3'}},
                'vendor': {'name': 'vendor_1'},
            },
            {
                'name': 'product_5',
                'brand': {'name': 'brand_3', 'image': {'src': 'brand_image_3'}},
                'vendor': {'name': 'vendor_2'},
            },
            {
                'name': 'product_6',
                'brand': {'name': 'brand_3', 'image': {'src': 'brand_image_3'}},
                'vendor': {'name': 'vendor_3'},
            },
            {
                'name': 'product_7',
                'brand': {'name': 'brand_4', 'image': {'src': 'brand_image_4'}},
                'vendor': {'name': 'vendor_1'},
            },
        ])

    async def test_store_select_related_m2m(self):
        with self.assertRaisesRegex(FieldError, "select_related only works with ForeignKey or OneToOneFields"):
            products = await Product.all().select_related('categories').limit(7)
