import uuid
from enum import Enum, IntEnum

from tortoise import fields
from tortoise.models import Model


class IntFields(Model):
    id = fields.IntegerField(primary_key=True)
    intnum = fields.IntegerField()
    intnum_null = fields.IntegerField(null=True)


class BigIntFields(Model):
    id = fields.BigIntegerField(primary_key=True)
    intnum = fields.BigIntegerField()
    intnum_null = fields.BigIntegerField(null=True)


class SmallIntFields(Model):
    id = fields.IntegerField(primary_key=True)
    smallintnum = fields.SmallIntegerField()
    smallintnum_null = fields.SmallIntegerField(null=True)


class CharFields(Model):
    id = fields.IntegerField(primary_key=True)
    char = fields.CharField(max_length=255)
    char_null = fields.CharField(max_length=255, null=True)


class TextFields(Model):
    id = fields.IntegerField(primary_key=True)
    text = fields.TextField()
    text_null = fields.TextField(null=True)


class BooleanFields(Model):
    id = fields.IntegerField(primary_key=True)
    boolean = fields.BooleanField()
    boolean_null = fields.BooleanField(null=True)


class BinaryFields(Model):
    id = fields.IntegerField(primary_key=True)
    binary = fields.BinaryField()
    binary_null = fields.BinaryField(null=True)


class DecimalFields(Model):
    class Meta:
        ordering = ['id', ]

    id = fields.IntegerField(primary_key=True)
    decimal = fields.DecimalField(max_digits=18, decimal_places=4)
    decimal_nodec = fields.DecimalField(max_digits=18, decimal_places=0)
    decimal_null = fields.DecimalField(max_digits=18, decimal_places=4, null=True)


class DatetimeFields(Model):
    id = fields.IntegerField(primary_key=True)
    datetime = fields.DateTimeField()
    datetime_null = fields.DateTimeField(null=True)
    datetime_auto = fields.DateTimeField(auto_now=True)
    datetime_add = fields.DateTimeField(auto_now_add=True)


class TimeDeltaFields(Model):
    id = fields.IntegerField(primary_key=True)
    timedelta = fields.TimeDeltaField()
    timedelta_null = fields.TimeDeltaField(null=True)


class DateFields(Model):
    id = fields.IntegerField(primary_key=True)
    date = fields.DateField()
    date_null = fields.DateField(null=True)


class FloatFields(Model):
    id = fields.IntegerField(primary_key=True)
    floatnum = fields.FloatField()
    floatnum_null = fields.FloatField(null=True)


class JSONFields(Model):
    id = fields.IntegerField(primary_key=True)
    data = fields.JSONField()
    data_null = fields.JSONField(null=True)
    data_default = fields.JSONField(default={"a": 1})


class UUIDFields(Model):
    id = fields.UUIDField(primary_key=True, default=uuid.uuid1)
    data = fields.UUIDField()
    data_auto = fields.UUIDField(default=uuid.uuid4)
    data_null = fields.UUIDField(null=True)


class Service(IntEnum):
    python_programming = 1
    database_design = 2
    system_administration = 3


class Currency(str, Enum):
    HUF = "HUF"
    EUR = "EUR"
    USD = "USD"


class EnumFields(Model):
    service: Service = fields.IntEnumField(Service)
    currency: Currency = fields.CharEnumField(Currency, default=Currency.HUF)
