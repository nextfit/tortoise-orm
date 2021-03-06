"""
This is the testing Models — Duplicate 1
"""

from tortoise import fields
from tortoise.models import Model


class Tournament(Model):
    id = fields.IntegerField(primary_key=True)


class Event(Model):
    tournament = fields.ForeignKey("models.Tournament", related_name="events")


class Party(Model):
    tournament = fields.ForeignKey("models.Tournament", related_name="events")
