"""
Testing Models for a bad/wrong relation reference
The model 'Tour' does not exist
"""
from tortoise import fields
from tortoise.models import Model


class Tournament(Model):
    id = fields.IntField(primary_key=True)


class Event(Model):
    tournament = fields.ForeignKeyField("models.Tour", related_name="events")
