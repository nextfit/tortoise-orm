
from tests.testmodels.tournament import Tournament
from tortoise import fields
from tortoise.models import Model


class UniqueName(Model):
    name = fields.CharField(max_length=20, null=True, unique=True)


class UniqueTogetherFields(Model):
    id = fields.IntegerField(primary_key=True)
    first_name = fields.CharField(max_length=64)
    last_name = fields.CharField(max_length=64)

    class Meta:
        unique_together = ("first_name", "last_name")


class UniqueTogetherFieldsWithFK(Model):
    id = fields.IntegerField(primary_key=True)
    text = fields.CharField(max_length=64)
    tournament: fields.ForeignKeyRelation[Tournament] = fields.ForeignKey("models.Tournament")

    class Meta:
        unique_together = ("text", "tournament")
