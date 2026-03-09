from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class FundingSnapshot:
    ticker: str
    rate: Decimal
    timestamp: datetime
