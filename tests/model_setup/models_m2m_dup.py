

from tortoise import fields
from tortoise.models import Model


class Event(Model):
    participants = fields.ManyToManyField("models.Team", related_name="events", through="models.TeamEvent")


class Team(Model):
    id = fields.IntegerField(primary_key=True)


class TeamEvent(Model):
    te_event = fields.ForeignKey("models.Event")
    te_host = fields.ForeignKey("models.Team", related_name="hosting_events")
    te_guest = fields.ForeignKey("models.Team", related_name="asguest_events")
