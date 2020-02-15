import json
import uuid

from tests.testmodels import (
    Event,
    JSONFields,
    Reporter,
    SourceFields,
    StraightFields,
    Team,
    Tournament,
    UUIDFkRelatedModel,
    UUIDFkRelatedNullModel,
    UUIDM2MRelatedModel,
    UUIDPkModel,
)
from tortoise import Tortoise, fields
from tortoise.contrib import test
from tortoise.fields.relational import (
    BackwardFKRelation,
    ForeignKeyField,
    ManyToManyField,
    OneToOneField,
)


class TestBasic(test.TestCase):
    maxDiff = None

    async def test_describe_models_all_serializable(self):
        val = Tortoise.describe_models()
        json.dumps(val)
        self.assertIn("models.SourceFields", val.keys())
        self.assertIn("models.Event", val.keys())

    async def test_describe_models_all_not_serializable(self):
        val = Tortoise.describe_models(serializable=False)
        with self.assertRaisesRegex(TypeError, "not JSON serializable"):
            json.dumps(val)
        self.assertIn("models.SourceFields", val.keys())
        self.assertIn("models.Event", val.keys())

    async def test_describe_models_some(self):
        val = Tortoise.describe_models([Event, Tournament, Reporter, Team])
        self.assertEqual(
            {"models.Event", "models.Tournament", "models.Reporter", "models.Team"}, set(val.keys())
        )

    async def test_describe_model_straight(self):
        val = Tortoise.describe_model(StraightFields)

        self.assertEqual(
            val,
            {
                "name": "models.StraightFields",
                "app": "models",
                "table": "straightfields",
                "abstract": False,
                "description": "Straight auto-mapped fields",
                "unique_together": [["chars", "blip"]],
                "pk_field": {
                    "name": "eyedee",
                    "field_type": "IntField",
                    "db_column": "eyedee",
                    "db_field_types": {"": "INT"},
                    "python_type": "int",
                    "generated": True,
                    "nullable": False,
                    "unique": True,
                    "indexed": True,
                    "default": None,
                    "description": "Da PK",
                },
                "data_fields": [
                    {
                        "name": "chars",
                        "field_type": "CharField",
                        "db_column": "chars",
                        "db_field_types": {"": "VARCHAR(50)"},
                        "python_type": "str",
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": True,
                        "default": None,
                        "description": "Some chars",
                    },
                    {
                        "name": "blip",
                        "field_type": "CharField",
                        "db_column": "blip",
                        "db_field_types": {"": "VARCHAR(50)"},
                        "python_type": "str",
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": "BLIP",
                        "description": None,
                    },
                    {
                        "name": "fk_id",
                        "field_type": "IntField",
                        "db_column": "fk_id",
                        "db_field_types": {"": "INT"},
                        "python_type": "int",
                        "generated": False,
                        "nullable": True,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "Tree!",
                    },
                    {
                        "db_column": "o2o_id",
                        "db_field_types": {"": "INT"},
                        "default": None,
                        "description": "Line",
                        "field_type": "IntField",
                        "generated": False,
                        "indexed": True,
                        "name": "o2o_id",
                        "nullable": True,
                        "python_type": "int",
                        "unique": True,
                    },
                ],
                "fk_fields": [
                    {
                        "name": "fk",
                        "field_type": "ForeignKeyField",
                        "raw_field": "fk_id",
                        "python_type": "models.StraightFields",
                        "generated": False,
                        "nullable": True,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "Tree!",
                    }
                ],
                "backward_fk_fields": [
                    {
                        "name": "fkrev",
                        "field_type": "BackwardFKRelation",
                        "python_type": "models.StraightFields",
                        "generated": False,
                        "nullable": True,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "Tree!",
                    }
                ],
                "o2o_fields": [
                    {
                        "default": None,
                        "description": "Line",
                        "field_type": "OneToOneField",
                        "generated": False,
                        "indexed": True,
                        "name": "o2o",
                        "nullable": True,
                        "python_type": "models.StraightFields",
                        "raw_field": "o2o_id",
                        "unique": True,
                    }
                ],
                "backward_o2o_fields": [
                    {
                        "default": None,
                        "description": "Line",
                        "field_type": "BackwardOneToOneRelation",
                        "generated": False,
                        "indexed": False,
                        "name": "o2o_rev",
                        "nullable": True,
                        "python_type": "models.StraightFields",
                        "unique": False,
                    }
                ],
                "m2m_fields": [
                    {
                        "name": "rel_to",
                        "field_type": "ManyToManyField",
                        "python_type": "models.StraightFields",
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "M2M to myself",
                    },
                    {
                        "name": "rel_from",
                        "field_type": "ManyToManyField",
                        "python_type": "models.StraightFields",
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "M2M to myself",
                    },
                ],
            },
        )

    async def test_describe_model_straight_native(self):
        val = Tortoise.describe_model(StraightFields, serializable=False)

        self.assertEqual(
            val,
            {
                "name": "models.StraightFields",
                "app": "models",
                "table": "straightfields",
                "abstract": False,
                "description": "Straight auto-mapped fields",
                "unique_together": [["chars", "blip"]],
                "pk_field": {
                    "name": "eyedee",
                    "field_type": fields.IntField,
                    "db_column": "eyedee",
                    "db_field_types": {"": "INT"},
                    "python_type": int,
                    "generated": True,
                    "nullable": False,
                    "unique": True,
                    "indexed": True,
                    "default": None,
                    "description": "Da PK",
                },
                "data_fields": [
                    {
                        "name": "chars",
                        "field_type": fields.CharField,
                        "db_column": "chars",
                        "db_field_types": {"": "VARCHAR(50)"},
                        "python_type": str,
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": True,
                        "default": None,
                        "description": "Some chars",
                    },
                    {
                        "name": "blip",
                        "field_type": fields.CharField,
                        "db_column": "blip",
                        "db_field_types": {"": "VARCHAR(50)"},
                        "python_type": str,
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": "BLIP",
                        "description": None,
                    },
                    {
                        "name": "fk_id",
                        "field_type": fields.IntField,
                        "db_column": "fk_id",
                        "db_field_types": {"": "INT"},
                        "python_type": int,
                        "generated": False,
                        "nullable": True,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "Tree!",
                    },
                    {
                        "name": "o2o_id",
                        "field_type": fields.IntField,
                        "db_column": "o2o_id",
                        "db_field_types": {"": "INT"},
                        "python_type": int,
                        "generated": False,
                        "nullable": True,
                        "unique": True,
                        "indexed": True,
                        "default": None,
                        "description": "Line",
                    },
                ],
                "fk_fields": [
                    {
                        "name": "fk",
                        "field_type": ForeignKeyField,
                        "raw_field": "fk_id",
                        "python_type": StraightFields,
                        "generated": False,
                        "nullable": True,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "Tree!",
                    }
                ],
                "backward_fk_fields": [
                    {
                        "name": "fkrev",
                        "field_type": BackwardFKRelation,
                        "python_type": StraightFields,
                        "generated": False,
                        "nullable": True,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "Tree!",
                    }
                ],
                "o2o_fields": [
                    {
                        "default": None,
                        "description": "Line",
                        "field_type": OneToOneField,
                        "generated": False,
                        "indexed": True,
                        "name": "o2o",
                        "nullable": True,
                        "python_type": StraightFields,
                        "raw_field": "o2o_id",
                        "unique": True,
                    },
                ],
                "backward_o2o_fields": [
                    {
                        "default": None,
                        "description": "Line",
                        "field_type": fields.BackwardOneToOneRelation,
                        "generated": False,
                        "indexed": False,
                        "name": "o2o_rev",
                        "nullable": True,
                        "python_type": StraightFields,
                        "unique": False,
                    },
                ],
                "m2m_fields": [
                    {
                        "name": "rel_to",
                        "field_type": ManyToManyField,
                        "python_type": StraightFields,
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "M2M to myself",
                    },
                    {
                        "name": "rel_from",
                        "field_type": ManyToManyField,
                        "python_type": StraightFields,
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "M2M to myself",
                    },
                ],
            },
        )

    async def test_describe_model_source(self):
        val = Tortoise.describe_model(SourceFields)

        self.assertEqual(
            val,
            {
                "name": "models.SourceFields",
                "app": "models",
                "table": "sometable",
                "abstract": False,
                "description": "Source mapped fields",
                "unique_together": [["chars", "blip"]],
                "pk_field": {
                    "name": "eyedee",
                    "field_type": "IntField",
                    "db_column": "sometable_id",
                    "db_field_types": {"": "INT"},
                    "python_type": "int",
                    "generated": True,
                    "nullable": False,
                    "unique": True,
                    "indexed": True,
                    "default": None,
                    "description": "Da PK",
                },
                "data_fields": [
                    {
                        "name": "chars",
                        "field_type": "CharField",
                        "db_column": "some_chars_table",
                        "db_field_types": {"": "VARCHAR(50)"},
                        "python_type": "str",
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": True,
                        "default": None,
                        "description": "Some chars",
                    },
                    {
                        "name": "blip",
                        "field_type": "CharField",
                        "db_column": "da_blip",
                        "db_field_types": {"": "VARCHAR(50)"},
                        "python_type": "str",
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": "BLIP",
                        "description": None,
                    },
                    {
                        "name": "fk_id",
                        "field_type": "IntField",
                        "db_column": "fk_sometable",
                        "db_field_types": {"": "INT"},
                        "python_type": "int",
                        "generated": False,
                        "nullable": True,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "Tree!",
                    },
                    {
                        "name": "o2o_id",
                        "field_type": "IntField",
                        "db_column": "o2o_sometable",
                        "db_field_types": {"": "INT"},
                        "python_type": "int",
                        "generated": False,
                        "nullable": True,
                        "unique": True,
                        "indexed": True,
                        "default": None,
                        "description": "Line",
                    },
                ],
                "fk_fields": [
                    {
                        "name": "fk",
                        "field_type": "ForeignKeyField",
                        "raw_field": "fk_id",
                        "python_type": "models.SourceFields",
                        "generated": False,
                        "nullable": True,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "Tree!",
                    }
                ],
                "backward_fk_fields": [
                    {
                        "name": "fkrev",
                        "field_type": "BackwardFKRelation",
                        "python_type": "models.SourceFields",
                        "generated": False,
                        "nullable": True,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "Tree!",
                    }
                ],
                "o2o_fields": [
                    {
                        "default": None,
                        "description": "Line",
                        "field_type": "OneToOneField",
                        "generated": False,
                        "indexed": True,
                        "name": "o2o",
                        "nullable": True,
                        "python_type": "models.SourceFields",
                        "raw_field": "o2o_id",
                        "unique": True,
                    }
                ],
                "backward_o2o_fields": [
                    {
                        "default": None,
                        "description": "Line",
                        "field_type": "BackwardOneToOneRelation",
                        "generated": False,
                        "indexed": False,
                        "name": "o2o_rev",
                        "nullable": True,
                        "python_type": "models.SourceFields",
                        "unique": False,
                    }
                ],
                "m2m_fields": [
                    {
                        "name": "rel_to",
                        "field_type": "ManyToManyField",
                        "python_type": "models.SourceFields",
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "M2M to myself",
                    },
                    {
                        "name": "rel_from",
                        "field_type": "ManyToManyField",
                        "python_type": "models.SourceFields",
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "M2M to myself",
                    },
                ],
            },
        )

    async def test_describe_model_source_native(self):
        val = Tortoise.describe_model(SourceFields, serializable=False)

        self.assertEqual(
            val,
            {
                "name": "models.SourceFields",
                "app": "models",
                "table": "sometable",
                "abstract": False,
                "description": "Source mapped fields",
                "unique_together": [["chars", "blip"]],
                "pk_field": {
                    "name": "eyedee",
                    "field_type": fields.IntField,
                    "db_column": "sometable_id",
                    "db_field_types": {"": "INT"},
                    "python_type": int,
                    "generated": True,
                    "nullable": False,
                    "unique": True,
                    "indexed": True,
                    "default": None,
                    "description": "Da PK",
                },
                "data_fields": [
                    {
                        "name": "chars",
                        "field_type": fields.CharField,
                        "db_column": "some_chars_table",
                        "db_field_types": {"": "VARCHAR(50)"},
                        "python_type": str,
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": True,
                        "default": None,
                        "description": "Some chars",
                    },
                    {
                        "name": "blip",
                        "field_type": fields.CharField,
                        "db_column": "da_blip",
                        "db_field_types": {"": "VARCHAR(50)"},
                        "python_type": str,
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": "BLIP",
                        "description": None,
                    },
                    {
                        "name": "fk_id",
                        "field_type": fields.IntField,
                        "db_column": "fk_sometable",
                        "db_field_types": {"": "INT"},
                        "python_type": int,
                        "generated": False,
                        "nullable": True,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "Tree!",
                    },
                    {
                        "name": "o2o_id",
                        "field_type": fields.IntField,
                        "db_column": "o2o_sometable",
                        "db_field_types": {"": "INT"},
                        "python_type": int,
                        "generated": False,
                        "nullable": True,
                        "unique": True,
                        "indexed": True,
                        "default": None,
                        "description": "Line",
                    },
                ],
                "fk_fields": [
                    {
                        "name": "fk",
                        "field_type": ForeignKeyField,
                        "raw_field": "fk_id",
                        "python_type": SourceFields,
                        "generated": False,
                        "nullable": True,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "Tree!",
                    }
                ],
                "backward_fk_fields": [
                    {
                        "name": "fkrev",
                        "field_type": BackwardFKRelation,
                        "python_type": SourceFields,
                        "generated": False,
                        "nullable": True,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "Tree!",
                    }
                ],
                "o2o_fields": [
                    {
                        "default": None,
                        "description": "Line",
                        "field_type": OneToOneField,
                        "generated": False,
                        "indexed": True,
                        "name": "o2o",
                        "nullable": True,
                        "python_type": SourceFields,
                        "raw_field": "o2o_id",
                        "unique": True,
                    }
                ],
                "backward_o2o_fields": [
                    {
                        "default": None,
                        "description": "Line",
                        "field_type": fields.BackwardOneToOneRelation,
                        "generated": False,
                        "indexed": False,
                        "name": "o2o_rev",
                        "nullable": True,
                        "python_type": SourceFields,
                        "unique": False,
                    }
                ],
                "m2m_fields": [
                    {
                        "name": "rel_to",
                        "field_type": ManyToManyField,
                        "python_type": SourceFields,
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "M2M to myself",
                    },
                    {
                        "name": "rel_from",
                        "field_type": ManyToManyField,
                        "python_type": SourceFields,
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": "M2M to myself",
                    },
                ],
            },
        )

    async def test_describe_model_uuidpk(self):
        val = Tortoise.describe_model(UUIDPkModel)

        self.assertEqual(
            val,
            {
                "name": "models.UUIDPkModel",
                "app": "models",
                "table": "uuidpkmodel",
                "abstract": False,
                "description": None,
                "unique_together": [],
                "pk_field": {
                    "name": "id",
                    "field_type": "UUIDField",
                    "db_column": "id",
                    "db_field_types": {"": "CHAR(36)", "postgres": "UUID"},
                    "python_type": "uuid.UUID",
                    "generated": False,
                    "nullable": False,
                    "unique": True,
                    "indexed": True,
                    "default": "<function uuid.uuid4>",
                    "description": None,
                },
                "data_fields": [],
                "fk_fields": [],
                "backward_fk_fields": [
                    {
                        "name": "children",
                        "field_type": "BackwardFKRelation",
                        "python_type": "models.UUIDFkRelatedModel",
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": None,
                    },
                ],
                "o2o_fields": [],
                "backward_o2o_fields": [],
                "m2m_fields": [
                    {
                        "name": "peers",
                        "field_type": "ManyToManyField",
                        "python_type": "models.UUIDM2MRelatedModel",
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": None,
                    }
                ],
            },
        )

    async def test_describe_model_uuidpk_native(self):
        val = Tortoise.describe_model(UUIDPkModel, serializable=False)

        self.assertEqual(
            val,
            {
                "name": "models.UUIDPkModel",
                "app": "models",
                "table": "uuidpkmodel",
                "abstract": False,
                "description": None,
                "unique_together": [],
                "pk_field": {
                    "name": "id",
                    "field_type": fields.UUIDField,
                    "db_column": "id",
                    "db_field_types": {"": "CHAR(36)", "postgres": "UUID"},
                    "python_type": uuid.UUID,
                    "generated": False,
                    "nullable": False,
                    "unique": True,
                    "indexed": True,
                    "default": uuid.uuid4,
                    "description": None,
                },
                "data_fields": [],
                "fk_fields": [],
                "backward_fk_fields": [
                    {
                        "name": "children",
                        "field_type": BackwardFKRelation,
                        "python_type": UUIDFkRelatedModel,
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": None,
                    },
                ],
                "o2o_fields": [],
                "backward_o2o_fields": [],
                "m2m_fields": [
                    {
                        "name": "peers",
                        "field_type": ManyToManyField,
                        "python_type": UUIDM2MRelatedModel,
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": None,
                    }
                ],
            },
        )

    async def test_describe_model_uuidpk_relatednull(self):
        val = Tortoise.describe_model(UUIDFkRelatedNullModel, serializable=True)

        self.assertEqual(
            val,
            {
                "abstract": False,
                "app": "models",
                "backward_fk_fields": [],
                "backward_o2o_fields": [],
                "data_fields": [
                    {
                        "db_column": "name",
                        "db_field_types": {"": "VARCHAR(50)"},
                        "default": None,
                        "description": None,
                        "field_type": "CharField",
                        "generated": False,
                        "indexed": False,
                        "name": "name",
                        "nullable": True,
                        "python_type": "str",
                        "unique": False,
                    },
                    {
                        "db_column": "model_id",
                        "db_field_types": {"": "CHAR(36)", "postgres": "UUID"},
                        "default": None,
                        "description": None,
                        "field_type": "UUIDField",
                        "generated": False,
                        "indexed": False,
                        "name": "model_id",
                        "nullable": True,
                        "python_type": "uuid.UUID",
                        "unique": False,
                    },
                    {
                        "db_column": "parent_id",
                        "db_field_types": {"": "CHAR(36)", "postgres": "UUID"},
                        "default": None,
                        "description": None,
                        "field_type": "UUIDField",
                        "generated": False,
                        "indexed": True,
                        "name": "parent_id",
                        "nullable": True,
                        "python_type": "uuid.UUID",
                        "unique": True,
                    },
                ],
                "description": None,
                "fk_fields": [
                    {
                        "default": None,
                        "description": None,
                        "field_type": "ForeignKeyField",
                        "generated": False,
                        "indexed": False,
                        "name": "model",
                        "nullable": True,
                        "python_type": "models.UUIDPkModel",
                        "raw_field": "model_id",
                        "unique": False,
                    }
                ],
                "m2m_fields": [],
                "name": "models.UUIDFkRelatedNullModel",
                "o2o_fields": [
                    {
                        "default": None,
                        "description": None,
                        "field_type": "OneToOneField",
                        "generated": False,
                        "indexed": True,
                        "name": "parent",
                        "nullable": True,
                        "python_type": "models.UUIDPkModel",
                        "raw_field": "parent_id",
                        "unique": True,
                    }
                ],
                "pk_field": {
                    "db_column": "id",
                    "db_field_types": {"": "CHAR(36)", "postgres": "UUID"},
                    "default": "<function uuid.uuid4>",
                    "description": None,
                    "field_type": "UUIDField",
                    "generated": False,
                    "indexed": True,
                    "name": "id",
                    "nullable": False,
                    "python_type": "uuid.UUID",
                    "unique": True,
                },
                "table": "uuidfkrelatednullmodel",
                "unique_together": [],
            },
        )

    async def test_describe_model_json(self):
        val = Tortoise.describe_model(JSONFields)

        self.assertEqual(
            val,
            {
                "name": "models.JSONFields",
                "app": "models",
                "table": "jsonfields",
                "abstract": False,
                "description": None,
                "unique_together": [],
                "pk_field": {
                    "name": "id",
                    "field_type": "IntField",
                    "db_column": "id",
                    "db_field_types": {"": "INT"},
                    "python_type": "int",
                    "generated": True,
                    "nullable": False,
                    "unique": True,
                    "indexed": True,
                    "default": None,
                    "description": None,
                },
                "data_fields": [
                    {
                        "name": "data",
                        "field_type": "JSONField",
                        "db_column": "data",
                        "db_field_types": {"": "TEXT", "postgres": "JSONB"},
                        "python_type": ["dict", "list"],
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": None,
                    },
                    {
                        "name": "data_null",
                        "field_type": "JSONField",
                        "db_column": "data_null",
                        "db_field_types": {"": "TEXT", "postgres": "JSONB"},
                        "python_type": ["dict", "list"],
                        "generated": False,
                        "nullable": True,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": None,
                    },
                    {
                        "name": "data_default",
                        "field_type": "JSONField",
                        "db_column": "data_default",
                        "db_field_types": {"": "TEXT", "postgres": "JSONB"},
                        "python_type": ["dict", "list"],
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": "{'a': 1}",
                        "description": None,
                    },
                ],
                "fk_fields": [],
                "backward_fk_fields": [],
                "o2o_fields": [],
                "backward_o2o_fields": [],
                "m2m_fields": [],
            },
        )

    async def test_describe_model_json_native(self):
        val = Tortoise.describe_model(JSONFields, serializable=False)

        self.assertEqual(
            val,
            {
                "name": "models.JSONFields",
                "app": "models",
                "table": "jsonfields",
                "abstract": False,
                "description": None,
                "unique_together": [],
                "pk_field": {
                    "name": "id",
                    "field_type": fields.IntField,
                    "db_column": "id",
                    "db_field_types": {"": "INT"},
                    "python_type": int,
                    "generated": True,
                    "nullable": False,
                    "unique": True,
                    "indexed": True,
                    "default": None,
                    "description": None,
                },
                "data_fields": [
                    {
                        "name": "data",
                        "field_type": fields.JSONField,
                        "db_column": "data",
                        "db_field_types": {"": "TEXT", "postgres": "JSONB"},
                        "python_type": (dict, list),
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": None,
                    },
                    {
                        "name": "data_null",
                        "field_type": fields.JSONField,
                        "db_column": "data_null",
                        "db_field_types": {"": "TEXT", "postgres": "JSONB"},
                        "python_type": (dict, list),
                        "generated": False,
                        "nullable": True,
                        "unique": False,
                        "indexed": False,
                        "default": None,
                        "description": None,
                    },
                    {
                        "name": "data_default",
                        "field_type": fields.JSONField,
                        "db_column": "data_default",
                        "db_field_types": {"": "TEXT", "postgres": "JSONB"},
                        "python_type": (dict, list),
                        "generated": False,
                        "nullable": False,
                        "unique": False,
                        "indexed": False,
                        "default": {"a": 1},
                        "description": None,
                    },
                ],
                "fk_fields": [],
                "backward_fk_fields": [],
                "o2o_fields": [],
                "backward_o2o_fields": [],
                "m2m_fields": [],
            },
        )
