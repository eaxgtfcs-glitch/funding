"""
Общие фикстуры для unit-тестов.
"""
from datetime import datetime, timezone
from decimal import Decimal

from app.connectors.model.position import Position
from app.connectors.model.state import ExchangeState


def make_position(
        ticker: str = "BTCUSDT",
        exchange_name: str = "test_exchange",
        direction: str = "long",
        amount: str = "1.0",
        avg_price: str = "50000",
        current_price: str = "51000",
) -> Position:
    return Position(
        ticker=ticker,
        exchange_name=exchange_name,
        direction=direction,
        amount=Decimal(amount),
        avg_price=Decimal(avg_price),
        current_price=Decimal(current_price),
    )


def make_state(name: str = "test_exchange") -> ExchangeState:
    now = datetime.now(tz=timezone.utc)
    return ExchangeState(
        name=name,
        positions={},
        maintenance_margin=Decimal(0),
        current_margin=Decimal(0),
        positions_update_time=now,
        maintenance_margin_update_time=now,
    )
