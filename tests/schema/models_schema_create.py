"""
This example demonstrates SQL Schema generation for each DB type supported.
"""
from tortoise import fields
from tortoise.models import Model


class Tournament(Model):
    tid = fields.SmallIntField(primary_key=True)
    name = fields.CharField(max_length=100, description="Tournament name", db_index=True)
    created = fields.DatetimeField(auto_now_add=True, description="Created */'`/* datetime")

    class Meta:
        table_description = "What Tournaments */'`/* we have"


class Event(Model):
    id = fields.BigIntField(primary_key=True, description="Event ID")
    name = fields.TextField()
    tournament = fields.ForeignKeyField(
        "models.Tournament", related_name="events", description="FK to tournament"
    )
    participants = fields.ManyToManyField(
        "models.Team",
        related_name="events",
        through="teamevents",
        description="How participants relate",
    )
    modified = fields.DatetimeField(auto_now=True)
    prize = fields.DecimalField(max_digits=10, decimal_places=2, null=True)
    token = fields.CharField(max_length=100, description="Unique token", unique=True)
    key = fields.CharField(max_length=100)

    class Meta:
        table_description = "This table contains a list of all the events"
        unique_together = [("name", "prize"), ["tournament", "key"]]


class Team(Model):
    name = fields.CharField(max_length=50, primary_key=True, description="The TEAM name (and PK)")
    key = fields.IntField()
    manager = fields.ForeignKeyField("models.Team", related_name="team_members", null=True)
    talks_to = fields.ManyToManyField("models.Team", related_name="gets_talked_to")

    class Meta:
        table_description = "The TEAMS!"
        indexes = [("manager", "key"), ["manager_id", "name"]]


class TeamAddress(Model):
    city = fields.CharField(max_length=50, description="City")
    country = fields.CharField(max_length=50, description="Country")
    street = fields.CharField(max_length=128, description="Street Address")
    team = fields.OneToOneField(
        "models.Team", related_name="address", on_delete=fields.CASCADE, primary_key=True
    )


class VenueInformation(Model):
    name = fields.CharField(max_length=128)
    capacity = fields.IntField()
    rent = fields.FloatField()
    team = fields.OneToOneField("models.Team", on_delete=fields.SET_NULL, null=True)


class SourceFields(Model):
    id = fields.IntField(primary_key=True, db_column="sometable_id")
    chars = fields.CharField(max_length=255, db_column="some_chars_table", db_index=True)

    fk = fields.ForeignKeyField(
        "models.SourceFields", related_name="team_members", null=True, db_column="fk_sometable"
    )

    rel_to = fields.ManyToManyField(
        "models.SourceFields",
        related_name="rel_from",
        through="sometable_self",
        forward_key="sts_forward",
        backward_key="backward_sts",
    )

    class Meta:
        table = "sometable"
        indexes = [["chars"]]


class DefaultPK(Model):
    val = fields.IntField()


class ZeroMixin:
    zero = fields.IntField()


class OneMixin(ZeroMixin):
    one = fields.CharField(40, null=True)


class TwoMixin:
    two = fields.CharField(40)


class AbstractModel(Model, OneMixin):
    new_field = fields.CharField(max_length=100)

    class Meta:
        abstract = True


class InheritedModel(AbstractModel, TwoMixin):
    name = fields.TextField()
