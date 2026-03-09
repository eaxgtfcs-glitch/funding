"""
Общие фикстуры для unit-тестов.
"""
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from app.connectors.model.funding import FundingSnapshot
from app.connectors.model.position import Position
from app.connectors.model.state import ExchangeState


def make_position(
        ticker: str = "BTCUSDT",
        exchange_name: str = "test_exchange",
        direction: str = "long",
        amount: str = "1.0",
        avg_price: str = "50000",
        current_price: str = "51000",
        funding_rate: Optional[str] = None,
) -> Position:
    return Position(
        ticker=ticker,
        exchange_name=exchange_name,
        direction=direction,
        amount=Decimal(amount),
        avg_price=Decimal(avg_price),
        current_price=Decimal(current_price),
        funding_rate=Decimal(funding_rate) if funding_rate is not None else None,
    )


def make_state(name: str = "test_exchange") -> ExchangeState:
    now = datetime.now(tz=timezone.utc)
    return ExchangeState(
        name=name,
        positions={},
        funding_rates={},
        funding_rates_history={},
        maintenance_margin=Decimal(0),
        current_margin=Decimal(0),
        positions_update_time=now,
        maintenance_margin_update_time=now,
        funding_rates_update_time=now,
    )


def make_funding_snapshot(
        ticker: str = "BTCUSDT",
        rate: str = "0.0001",
) -> FundingSnapshot:
    return FundingSnapshot(
        ticker=ticker,
        rate=Decimal(rate),
        timestamp=datetime.now(tz=timezone.utc),
    )
