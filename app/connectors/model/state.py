from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from app.connectors.model.funding import FundingSnapshot
from app.connectors.model.position import Position


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass
class ExchangeState:
    name: str
    positions: dict[str, Position]
    funding_rates: dict[str, FundingSnapshot]
    funding_rates_history: dict[str, list[FundingSnapshot]]
    maintenance_margin: Decimal
    current_margin: Decimal
    positions_update_time: datetime
    maintenance_margin_update_time: datetime
    funding_rates_update_time: datetime
