import logging
import os
import zoneinfo
from dataclasses import dataclass
from datetime import timezone, tzinfo

_logger = logging.getLogger(__name__)


@dataclass
class ConnectorConfig:
    positions_interval: int = int(os.environ.get("POSITION_UPDATE_INTERVAL", "5"))
    margin_interval: int = int(os.environ.get("MARGIN_UPDATE_INTERVAL", "5"))


DEFAULT_CONFIG = ConnectorConfig()

CRITICAL_ALERT_SEND_COUNT: int = int(os.environ.get("CRITICAL_ALERT_SEND_COUNT", "3"))
CRITICAL_ALERT_REPEAT_INTERVAL: int = int(os.environ.get("CRITICAL_ALERT_REPEAT_INTERVAL", "5"))
NOTIFY_TIMEZONE = os.environ.get("NOTIFY_TIMEZONE", "UTC")


def get_notify_tz() -> tzinfo:
    if NOTIFY_TIMEZONE in ("UTC", "utc", ""):
        return timezone.utc
    try:
        return zoneinfo.ZoneInfo(NOTIFY_TIMEZONE)
    except (zoneinfo.ZoneInfoNotFoundError, KeyError):
        _logger.warning("Invalid NOTIFY_TIMEZONE=%r, falling back to UTC", NOTIFY_TIMEZONE)
        return timezone.utc
