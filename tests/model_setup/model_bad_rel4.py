"""
Testing Models for a bad/wrong relation reference
Wrong reference. Two '.' in reference.
"""
from tortoise import fields
from tortoise.models import Model


class Tournament(Model):
    id = fields.IntegerField(primary_key=True)


class Event(Model):
    tournament = fields.ForeignKey("models.app.Tournament", related_name="events")
