"""
Testing Models for a bad/wrong relation reference
"""
from tortoise import fields
from tortoise.models import Model


class Tournament(Model):
    id = fields.IntField(primary_key=True)


class Event(Model):
    tournament = fields.ForeignKeyField("app.Tournament", related_name="events")
