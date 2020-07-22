
from unittest import TestCase
from tortoise import fields


class TestRequired(TestCase):

    def test_required_by_default(self):
        self.assertTrue(fields.Field().required)

    def test_if_generated_then_not_required(self):
        self.assertFalse(fields.Field(generated=True).required)

    def test_if_null_then_not_required(self):
        self.assertFalse(fields.Field(null=True).required)

    def test_if_has_non_null_default_then_not_required(self):
        self.assertFalse(fields.TextField(default="").required)

    def test_if_null_default_then_required(self):
        self.assertTrue(fields.TextField(default=None).required)
