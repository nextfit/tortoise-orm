from pypika.functions import Count

from tests.testmodels import Address, Event, Team, Tournament
from tests.testmodels.store import create_store_objects, Product, Category
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

    async def test_store_select_related_prefetch(self):
        await create_store_objects()

        # products = await Product.all().prefetch_related('categories')

        prefetch = Prefetch('categories', queryset=Category.all().select_related('image'))
        products = await Product.all().prefetch_related(prefetch)

        products_distilled = [
            {
                'name': p.name,
                'categories': [
                    {'name': c.name, 'image': {'src': c.image.src}} for c in p.categories
                ],
            }
            for p in products
        ]

        self.assertEqual(products_distilled, [
            {'name': 'product_1', 'categories': []},
            {'name': 'product_2', 'categories': [
                {'name': 'category_2', 'image': {'src': 'category_image_2'}}
            ]},
            {'name': 'product_3', 'categories': [
                {'name': 'category_3', 'image': {'src': 'category_image_3'}}
            ]},
            {'name': 'product_4', 'categories': [
                {'name': 'category_2', 'image': {'src': 'category_image_2'}}
            ]},
            {'name': 'product_5', 'categories': [
                {'name': 'category_5', 'image': {'src': 'category_image_5'}}
            ]},
            {'name': 'product_6', 'categories': [
                {'name': 'category_2', 'image': {'src': 'category_image_2'}},
                {'name': 'category_3', 'image': {'src': 'category_image_3'}}
            ]},
            {'name': 'product_7', 'categories': [
                {'name': 'category_7', 'image': {'src': 'category_image_7'}}
            ]},
            {'name': 'product_8', 'categories': [
                {'name': 'category_2', 'image': {'src': 'category_image_2'}}
            ]},
            {'name': 'product_9', 'categories': [
                {'name': 'category_3', 'image': {'src': 'category_image_3'}}
            ]},
            {'name': 'product_10', 'categories': [
                {'name': 'category_2', 'image': {'src': 'category_image_2'}},
                {'name': 'category_5', 'image': {'src': 'category_image_5'}}
            ]},
            {'name': 'product_11', 'categories': []},
            {'name': 'product_12', 'categories': [
                {'name': 'category_2', 'image': {'src': 'category_image_2'}},
                {'name': 'category_3', 'image': {'src': 'category_image_3'}}
            ]},
            {'name': 'product_13', 'categories': []},
            {'name': 'product_14', 'categories': [
                {'name': 'category_2', 'image': {'src': 'category_image_2'}},
                {'name': 'category_7', 'image': {'src': 'category_image_7'}}
            ]},
            {'name': 'product_15', 'categories': [
                {'name': 'category_3', 'image': {'src': 'category_image_3'}},
                {'name': 'category_5', 'image': {'src': 'category_image_5'}}
            ]},
            {'name': 'product_16', 'categories': [
                {'name': 'category_2', 'image': {'src': 'category_image_2'}}
            ]},
            {'name': 'product_17', 'categories': []},
            {'name': 'product_18', 'categories': [
                {'name': 'category_2', 'image': {'src': 'category_image_2'}},
                {'name': 'category_3', 'image': {'src': 'category_image_3'}}
            ]},
            {'name': 'product_19', 'categories': []},
            {'name': 'product_20', 'categories': [
                {'name': 'category_2', 'image': {'src': 'category_image_2'}},
                {'name': 'category_5', 'image': {'src': 'category_image_5'}}
            ]},
            {'name': 'product_21', 'categories': [
                {'name': 'category_3', 'image': {'src': 'category_image_3'}},
                {'name': 'category_7', 'image': {'src': 'category_image_7'}}
            ]},
        ])
