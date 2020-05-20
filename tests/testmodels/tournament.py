
import binascii
import os

from tortoise import fields
from tortoise.models import Model


def generate_token():
    return binascii.hexlify(os.urandom(16)).decode("ascii")


class Tournament(Model):
    id = fields.SmallIntegerField(primary_key=True)
    name = fields.CharField(max_length=255)
    desc = fields.TextField(null=True)
    created = fields.DateTimeField(auto_now_add=True, db_index=True)

    events: fields.ReverseRelation["Event"]
    minrelations: fields.ReverseRelation["MinRelation"]
    uniquetogetherfieldswithfks: fields.ReverseRelation["UniqueTogetherFieldsWithFK"]

    def __str__(self):
        return self.name


class Reporter(Model):
    id = fields.IntegerField(primary_key=True)
    name = fields.TextField()

    events: fields.ReverseRelation["Event"]

    class Meta:
        db_table = "re_port_er"

    def __str__(self):
        return self.name


class Event(Model):
    id = fields.BigIntegerField(primary_key=True)
    name = fields.TextField()
    tournament: fields.ForeignKeyRelation["Tournament"] = fields.ForeignKey(
        "models.Tournament", related_name="events"
    )
    reporter: fields.ForeignKeyNullableRelation[Reporter] = fields.ForeignKey(
        "models.Reporter", null=True
    )
    participants: fields.ManyToManyRelation["Team"] = fields.ManyToManyField(
        "models.Team", related_name="events", through="event_team", backward_key="idEvent"
    )
    modified = fields.DateTimeField(auto_now=True)
    token = fields.TextField(default=generate_token)
    alias = fields.IntegerField(null=True)

    def __str__(self):
        return self.name


class Address(Model):
    city = fields.CharField(max_length=64)
    street = fields.CharField(max_length=128)

    event: fields.OneToOneRelation[Event] = fields.OneToOneField(
        "models.Event", on_delete=fields.CASCADE, related_name="address", null=True
    )


class Team(Model):
    id = fields.IntegerField(primary_key=True)
    name = fields.TextField()

    events: fields.ManyToManyRelation[Event]
    minrelation_through: fields.ManyToManyRelation["MinRelation"]
    alias = fields.IntegerField(null=True)

    def __str__(self):
        return self.name


class EventTwo(Model):
    id = fields.IntegerField(primary_key=True)
    name = fields.TextField()
    tournament_id = fields.IntegerField()
    # Here we make link to events.Team, not models.Team
    participants: fields.ManyToManyRelation["TeamTwo"] = fields.ManyToManyField("events.TeamTwo")

    class Meta:
        app = "events"

    def __str__(self):
        return self.name


class TeamTwo(Model):
    id = fields.IntegerField(primary_key=True)
    name = fields.TextField()

    eventtwo_through: fields.ManyToManyRelation[EventTwo]

    class Meta:
        app = "events"

    def __str__(self):
        return self.name


class MinRelation(Model):
    id = fields.IntegerField(primary_key=True)
    tournament: fields.ForeignKeyRelation[Tournament] = fields.ForeignKey("models.Tournament")
    participants: fields.ManyToManyRelation[Team] = fields.ManyToManyField("models.Team")
