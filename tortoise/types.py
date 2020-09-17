
from enum import Enum


class CharEnum(str, Enum):
    """Enum where members are also (and must be) ints"""
