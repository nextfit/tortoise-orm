
from tortoise import fields
from tortoise.models import Model


class ImplicitPkModel(Model):
    value = fields.TextField()


class CharPkModel(Model):
    id = fields.CharField(max_length=64, primary_key=True)


class CharFkRelatedModel(Model):
    model = fields.ForeignKey(CharPkModel, related_name="children")


class CharM2MRelatedModel(Model):
    value = fields.TextField(default="test")
    models = fields.ManyToManyField("models.CharPkModel", related_name="peers")


class DoubleFK(Model):
    name = fields.CharField(max_length=50)
    left = fields.ForeignKey("models.DoubleFK", null=True, related_name="left_rel")
    right = fields.ForeignKey("DoubleFK", null=True, related_name="right_rel")


class M2MOne(Model):
    id = fields.IntegerField(primary_key=True)
    name = fields.CharField(max_length=255, null=True)
    two: fields.ManyToManyRelation["M2MTwo"] = fields.ManyToManyField(
        "models.M2MTwo", related_name="one"
    )


class M2MTwo(Model):
    id = fields.IntegerField(primary_key=True)
    name = fields.CharField(max_length=255, null=True)

    one: fields.ManyToManyRelation[M2MOne]


class StraightFields(Model):
    eyedee = fields.IntegerField(primary_key=True, description="Da PK")
    chars = fields.CharField(max_length=50, db_index=True, description="Some chars")
    blip = fields.CharField(max_length=50, default="BLIP")

    fk: fields.ForeignKeyNullableRelation["StraightFields"] = fields.ForeignKey(
        "models.StraightFields", related_name="fkrev", null=True, description="Tree!"
    )
    fkrev: fields.ReverseRelation["StraightFields"]

    o2o: fields.OneToOneNullableRelation["StraightFields"] = fields.OneToOneField(
        "models.StraightFields", related_name="o2o_rev", null=True, description="Line"
    )
    o2o_rev: fields.Field

    rel_to: fields.ManyToManyRelation["StraightFields"] = fields.ManyToManyField(
        "models.StraightFields", related_name="rel_from", description="M2M to myself"
    )
    rel_from: fields.ManyToManyRelation["StraightFields"]

    class Meta:
        unique_together = [["chars", "blip"]]
        table_description = "Straight auto-mapped fields"


class SourceFields(Model):
    eyedee = fields.IntegerField(primary_key=True, db_column="sometable_id", description="Da PK")
    chars = fields.CharField(
        max_length=50, db_column="some_chars_table", db_index=True, description="Some chars"
    )
    blip = fields.CharField(max_length=50, default="BLIP", db_column="da_blip")

    fk: fields.ForeignKeyNullableRelation["SourceFields"] = fields.ForeignKey(
        "models.SourceFields",
        related_name="fkrev",
        null=True,
        db_column="fk_sometable",
        description="Tree!",
    )
    fkrev: fields.ReverseRelation["SourceFields"]

    o2o: fields.OneToOneNullableRelation["SourceFields"] = fields.OneToOneField(
        "models.SourceFields",
        related_name="o2o_rev",
        null=True,
        db_column="o2o_sometable",
        description="Line",
    )
    o2o_rev: fields.Field

    rel_to: fields.ManyToManyRelation["SourceFields"] = fields.ManyToManyField(
        "models.SourceFields",
        related_name="rel_from",
        through="sometable_self",
        forward_key="sts_forward",
        backward_key="backward_sts",
        description="M2M to myself",
    )
    rel_from: fields.ManyToManyRelation["SourceFields"]

    class Meta:
        db_table = "sometable"
        unique_together = [["chars", "blip"]]
        table_description = "Source mapped fields"


class Employee(Model):
    name = fields.CharField(max_length=50)

    manager: fields.ForeignKeyNullableRelation["Employee"] = fields.ForeignKey(
        "models.Employee", related_name="team_members", null=True
    )
    team_members: fields.ReverseRelation["Employee"]

    talks_to: fields.ManyToManyRelation["Employee"] = fields.ManyToManyField(
        "models.Employee", related_name="gets_talked_to"
    )
    gets_talked_to: fields.ManyToManyRelation["Employee"]

    def __str__(self):
        return self.name

    async def full_hierarchy__async_for(self, level=0):
        """
        Demonstrates ``async for` to fetch relations

        An async iterator will fetch the relationship on-demand.
        """
        text = [
            "{}{} (to: {}) (from: {})".format(
                level * "  ",
                self,
                ", ".join(sorted([str(val) async for val in self.talks_to])),
                ", ".join(sorted([str(val) async for val in self.gets_talked_to])),
            )
        ]
        async for member in self.team_members:
            text.append(await member.full_hierarchy__async_for(level + 1))
        return "\n".join(text)

    async def full_hierarchy__fetch_related(self, level=0):
        """
        Demonstrates ``await .fetch_related`` to fetch relations

        On prefetching the data, the relationship files will contain a regular list.

        This is how one would get relations working on sync serialisation/templating frameworks.
        """
        await self.fetch_related("team_members", "talks_to", "gets_talked_to")
        text = [
            "{}{} (to: {}) (from: {})".format(
                level * "  ",
                self,
                ", ".join(sorted([str(val) for val in self.talks_to])),
                ", ".join(sorted([str(val) for val in self.gets_talked_to])),
            )
        ]
        for member in self.team_members:
            text.append(await member.full_hierarchy__fetch_related(level + 1))
        return "\n".join(text)
