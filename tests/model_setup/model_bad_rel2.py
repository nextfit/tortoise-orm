"""
Testing Models for a bad/wrong relation reference
The model 'Tour' does not exist
"""
from tortoise import fields
from tortoise.models import Model


class Tournament(Model):
    id = fields.IntegerField(primary_key=True)


class Event(Model):
    tournament = fields.ForeignKey("models.Tour", related_name="events")
