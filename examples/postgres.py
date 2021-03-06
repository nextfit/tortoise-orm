"""
This example showcases postgres features
"""
from tortoise import Tortoise, fields, run_async
from tortoise.models import Model


class Report(Model):
    id = fields.IntegerField(primary_key=True)
    content = fields.JSONField()

    def __str__(self):
        return str(self.id)


async def run():
    Tortoise.init(
        {
            "connections": {
                "default": {
                    "engine": "tortoise.backends.asyncpg",
                    "host": "localhost",
                    "port": "5432",
                    "user": "tortoise",
                    "password": "qwerty123",
                    "database": "test",
                }
            },
            "apps": {"models": {"models": ["__main__"], "default_connection": "default"}},
        },
    )

    await Tortoise.open_connections(create_db=True)
    await Tortoise.generate_schemas()

    report_data = {"foo": "bar"}
    print(await Report.create(content=report_data))
    print(await Report.filter(content=report_data).first())
    await Tortoise.drop_databases()


if __name__ == "__main__":
    run_async(run())
