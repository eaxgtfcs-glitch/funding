from dataclasses import dataclass

from app.connectors.model.position import Position


@dataclass
class Pair:
    position_a: Position
    position_b: Position

    @property
    def is_valid(self) -> bool:
        """Positions must be on opposite sides."""
        return self.position_a.direction != self.position_b.direction

    def get_counterpart(self, exchange_name: str, ticker: str) -> Position | None:
        """Return the other position given one side of the pair."""
        if self.position_a.exchange_name == exchange_name and self.position_a.ticker == ticker:
            return self.position_b
        if self.position_b.exchange_name == exchange_name and self.position_b.ticker == ticker:
            return self.position_a
        return None
