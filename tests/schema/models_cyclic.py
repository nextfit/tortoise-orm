"""
This is the testing Models — Cyclic
"""
from tortoise import fields
from tortoise.models import Model


class One(Model):
    tournament = fields.ForeignKey("models.Two", related_name="events")


class Two(Model):
    tournament = fields.ForeignKey("models.Three", related_name="events")


class Three(Model):
    tournament = fields.ForeignKey("models.One", related_name="events")
