from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass
class Position:
    ticker: str
    exchange_name: str
    direction: Literal["long", "short"]
    amount: Decimal
    avg_price: Decimal
    current_price: Decimal
    funding_rate: Decimal | None
    update_time: datetime = field(default_factory=_utcnow)

    def __post_init__(self) -> None:
        if self.amount < 0:
            raise ValueError(f"Position amount must be >= 0, got {self.amount}")
