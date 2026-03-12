from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

from app.connectors.model.position import Position


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass
class ExchangeState:
    name: str
    positions: dict[str, Position]
    maintenance_margin: Decimal
    current_margin: Decimal
    positions_update_time: datetime
    maintenance_margin_update_time: datetime
    margin_ratio: Decimal | None = field(default=None)
