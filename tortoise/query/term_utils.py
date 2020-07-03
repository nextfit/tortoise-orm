

from pypika.terms import ArithmeticExpression, ValueWrapper
from pypika.terms import Function as PyPikaFunction, Term as PyPikaTerm

from tortoise.exceptions import ParamsError


def term_name(term: PyPikaTerm) -> str:
    if isinstance(term, ValueWrapper):
        return str(term.value)

    if isinstance(term, ArithmeticExpression):
        return "{}__{}__{}".format(term_name(term.left), str(term.operator), term_name(term.right))

    if isinstance(term, PyPikaFunction):
        return "{}__{}".format("__".join(map(term_name, term.args)), term.name.lower())

    raise ParamsError("Unable to find term name {}".format(term))
