
from tortoise import fields
from tortoise.models import Model


class TimestampMixin:
    created_at = fields.DateTimeField(null=True, auto_now_add=True)
    modified_at = fields.DateTimeField(null=True, auto_now=True)


class NameMixin:
    name = fields.CharField(40, unique=True)


class MyAbstractBaseModel(NameMixin, Model):
    id = fields.IntegerField(primary_key=True)

    class Meta:
        abstract = True


class MyDerivedModel(TimestampMixin, MyAbstractBaseModel):
    first_name = fields.CharField(20, null=True)
