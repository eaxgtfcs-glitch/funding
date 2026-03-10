import os
from dataclasses import dataclass


@dataclass
class ConnectorConfig:
    positions_interval: int = int(os.environ.get("POSITION_UPDATE_INTERVAL", "5"))
    margin_interval: int = int(os.environ.get("MARGIN_UPDATE_INTERVAL", "5"))
    funding_interval: int = int(os.environ.get("FUNDING_UPDATE_INTERVAL", "5"))


DEFAULT_CONFIG = ConnectorConfig()

CRITICAL_ALERT_SEND_COUNT: int = int(os.environ.get("CRITICAL_ALERT_SEND_COUNT", "3"))
CRITICAL_ALERT_REPEAT_INTERVAL: int = int(os.environ.get("CRITICAL_ALERT_REPEAT_INTERVAL", "5"))
