"""
This is the testing Models — Multiple PK
"""
from tortoise import fields
from tortoise.models import Model


class Tournament(Model):
    id = fields.IntegerField(primary_key=True)
    id2 = fields.IntegerField(primary_key=True)
