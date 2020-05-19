
from tortoise import fields
from tortoise.models import Model


class UUIDPkModel(Model):
    id = fields.UUIDField(primary_key=True)

    children: fields.ReverseRelation["UUIDFkRelatedModel"]
    children_null: fields.ReverseRelation["UUIDFkRelatedNullModel"]
    peers: fields.ManyToManyRelation["UUIDM2MRelatedModel"]


class UUIDFkRelatedModel(Model):
    id = fields.UUIDField(primary_key=True)
    name = fields.CharField(max_length=50, null=True)
    model: fields.ForeignKeyRelation[UUIDPkModel] = fields.ForeignKey(
        "models.UUIDPkModel", related_name="children"
    )


class UUIDFkRelatedNullModel(Model):
    id = fields.UUIDField(primary_key=True)
    name = fields.CharField(max_length=50, null=True)
    model: fields.ForeignKeyNullableRelation[UUIDPkModel] = fields.ForeignKey(
        "models.UUIDPkModel", related_name=False, null=True
    )
    parent: fields.OneToOneNullableRelation[UUIDPkModel] = fields.OneToOneField(
        "models.UUIDPkModel", related_name=False, null=True
    )


class UUIDM2MRelatedModel(Model):
    id = fields.UUIDField(primary_key=True)
    value = fields.TextField(default="test")
    models: fields.ManyToManyRelation[UUIDPkModel] = fields.ManyToManyField(
        "models.UUIDPkModel", related_name="peers"
    )


class UUIDPkSourceModel(Model):
    id = fields.UUIDField(primary_key=True, db_column="a")

    class Meta:
        db_table = "upsm"


class UUIDFkRelatedSourceModel(Model):
    id = fields.UUIDField(primary_key=True, db_column="b")
    name = fields.CharField(max_length=50, null=True, db_column="c")
    model = fields.ForeignKey(
        "models.UUIDPkSourceModel", related_name="children", db_column="d"
    )

    class Meta:
        db_table = "ufrsm"


class UUIDFkRelatedNullSourceModel(Model):
    id = fields.UUIDField(primary_key=True, db_column="i")
    name = fields.CharField(max_length=50, null=True, db_column="j")
    model = fields.ForeignKey(
        "models.UUIDPkSourceModel", related_name="children_null", db_column="k", null=True
    )

    class Meta:
        db_table = "ufrnsm"


class UUIDM2MRelatedSourceModel(Model):
    id = fields.UUIDField(primary_key=True, db_column="e")
    value = fields.TextField(default="test", db_column="f")
    models = fields.ManyToManyField(
        "models.UUIDPkSourceModel", related_name="peers", forward_key="e", backward_key="h"
    )

    class Meta:
        db_table = "umrsm"
