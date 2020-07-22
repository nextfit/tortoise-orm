
from tortoise import Tortoise, run_async
from tortoise.contrib.test import SimpleTestCase


class TestRunAsync(SimpleTestCase):
    def setUp(self):
        self.somevalue = 1

    async def init(self):
        Tortoise.init(db_url="sqlite://:memory:", modules={"models": []})
        self.somevalue = 2
        self.assertNotEqual(Tortoise._db_client_map, {})

    async def init_raise(self):
        Tortoise.init(db_url="sqlite://:memory:", modules={"models": []})
        self.somevalue = 3
        self.assertNotEqual(Tortoise._db_client_map, {})
        raise Exception("Some exception")

    def test_run_async(self):
        Tortoise._reset()
        self.assertEqual(Tortoise._db_client_map, {})
        self.assertEqual(self.somevalue, 1)
        run_async(self.init())
        self.assertEqual(self.somevalue, 2)

    def test_run_async_raised(self):
        Tortoise._reset()
        self.assertEqual(Tortoise._db_client_map, {})
        self.assertEqual(self.somevalue, 1)
        with self.assertRaises(Exception):
            run_async(self.init_raise())
        self.assertEqual(self.somevalue, 3)
