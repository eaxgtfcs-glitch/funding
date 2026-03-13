from dataclasses import dataclass, field
from decimal import Decimal


@dataclass
class StructureLeg:
    exchange: str
    ticker: str
    multiplier: Decimal = field(default_factory=lambda: Decimal("1"))


@dataclass
class Structure:
    legs: list[StructureLeg]
    is_active: bool
