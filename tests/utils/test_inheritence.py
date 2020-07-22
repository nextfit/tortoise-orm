
from unittest import TestCase
from tests.testmodels import MyAbstractBaseModel, MyDerivedModel


class TestInheritance(TestCase):
    def test_basic(self):
        self.assertTrue(hasattr(MyAbstractBaseModel(), "name"))
        self.assertTrue(hasattr(MyDerivedModel(), "created_at"))
        self.assertTrue(hasattr(MyDerivedModel(), "modified_at"))
        self.assertTrue(hasattr(MyDerivedModel(), "name"))
        self.assertTrue(hasattr(MyDerivedModel(), "first_name"))
