"""
Продовые тесты приложения funding — без биржевых API, без side-effects.

Покрывает:
  1. Импорт всех ключевых модулей
  2. Форматтеры — синтетические данные
  3. Timezone — get_notify_tz() при разных значениях NOTIFY_TIMEZONE
  4. TelegramAlertService — инициализация и get_updates без реального токена
"""
import asyncio
import importlib
import time
from datetime import datetime, timezone
from decimal import Decimal

import pytest


# ---------------------------------------------------------------------------
# Вспомогательные фабрики синтетических данных
# ---------------------------------------------------------------------------

def _make_state(name: str = "TestExchange", positions: dict | None = None,
                maintenance: str = "1000", current: str = "3500",
                ratio: str | None = "28.57"):
    from app.connectors.model.state import ExchangeState

    now = datetime.now(tz=timezone.utc)
    pos_dict = positions or {}
    return ExchangeState(
        name=name,
        positions=pos_dict,
        maintenance_margin=Decimal(maintenance),
        current_margin=Decimal(current),
        positions_update_time=now,
        maintenance_margin_update_time=now,
        margin_ratio=Decimal(ratio) if ratio is not None else None,
    )


def _make_position(ticker: str = "BTCUSDT", direction: str = "long",
                   amount: str = "0.5", avg_price: str = "60000",
                   current_price: str = "62000"):
    from app.connectors.model.position import Position
    return Position(
        ticker=ticker,
        exchange_name="TestExchange",
        direction=direction,
        amount=Decimal(amount),
        avg_price=Decimal(avg_price),
        current_price=Decimal(current_price)
    )


# ===========================================================================
# Тест 1 — Импорт модулей
# ===========================================================================

class TestModuleImports:

    def test_import_connectors_model(self):
        importlib.import_module("app.connectors.model.position")
        importlib.import_module("app.connectors.model.state")

    def test_import_connectors_config(self):
        importlib.import_module("app.connectors.config")

    def test_import_connectors_base(self):
        importlib.import_module("app.connectors.base")

    def test_import_telegram_formatters(self):
        importlib.import_module("app.telegram.formatters")

    def test_import_telegram_service(self):
        importlib.import_module("app.telegram.service")

    def test_import_telegram_state_broadcaster(self):
        importlib.import_module("app.telegram.state_broadcaster")

    def test_import_engine_model(self):
        importlib.import_module("app.engine.model.structure")

    def test_import_engine_engine(self):
        # engine.py импортируется без инстанциирования — проверяем только синтаксис
        importlib.import_module("app.engine.engine")


# ===========================================================================
# Тест 2 — Форматтеры
# ===========================================================================

class TestFormatters:

    def setup_method(self):
        from app.telegram import formatters
        self.fmt = formatters

    # --- format_margin_alert ---

    def test_format_margin_alert_contains_exchange_name(self):
        state = _make_state("Binance")
        result = self.fmt.format_margin_alert(state, Decimal("0.1"))
        assert "Binance" in result

    def test_format_margin_alert_contains_maintenance_value(self):
        state = _make_state(maintenance="1000", current="3500")
        result = self.fmt.format_margin_alert(state, Decimal("0.1"))
        assert "1000.00" in result

    def test_format_margin_alert_contains_current_value(self):
        state = _make_state(maintenance="1000", current="3500")
        result = self.fmt.format_margin_alert(state, Decimal("0.1"))
        assert "3500.00" in result

    def test_format_margin_alert_zero_maintenance_no_div_by_zero(self):
        state = _make_state(maintenance="0", current="3500")
        result = self.fmt.format_margin_alert(state, Decimal("0.1"))
        # 0 maintenance — remaining должен быть 0.0%
        assert "0.0%" in result

    def test_format_margin_alert_threshold_displayed(self):
        state = _make_state()
        result = self.fmt.format_margin_alert(state, Decimal("0.15"))
        assert "15%" in result

    # --- format_position_reduction_alert ---

    def test_format_position_reduction_alert_basic(self):
        result = self.fmt.format_position_reduction_alert(
            exchange_name="Bybit",
            ticker="BTCUSDT",
            old_amount=Decimal("1.0"),
            new_amount=Decimal("0.5"),
            counterpart=None,
        )
        assert "BTCUSDT" in result
        assert "Bybit" in result
        assert "1.0" in result
        assert "0.5" in result

    def test_format_position_reduction_alert_fully_closed(self):
        result = self.fmt.format_position_reduction_alert(
            exchange_name="Bybit",
            ticker="ETHUSDT",
            old_amount=Decimal("2.0"),
            new_amount=Decimal("0"),
            counterpart=None,
        )
        assert "fully closed" in result.lower() or "Fully closed" in result

    def test_format_position_reduction_alert_with_counterpart(self):
        cp = _make_position(ticker="ETHUSDT", direction="short")
        result = self.fmt.format_position_reduction_alert(
            exchange_name="Bybit",
            ticker="BTCUSDT",
            old_amount=Decimal("1.0"),
            new_amount=Decimal("0.8"),
            counterpart=cp,
        )
        assert "ETHUSDT" in result
        assert "Paired" in result

    # --- format_stale_connector_alert ---

    def test_format_stale_connector_alert(self):
        now = datetime.now(tz=timezone.utc)
        last = now.replace(second=now.second - 30 if now.second >= 30 else 0)
        result = self.fmt.format_stale_connector_alert("OKX", last, now)
        assert "OKX" in result
        assert "STALE CONNECTOR" in result

    # --- format_high_margin_ratio_alert ---

    def test_format_high_margin_ratio_alert_with_ratio(self):
        state = _make_state(name="Bybit", ratio="65.5")
        result = self.fmt.format_high_margin_ratio_alert(state)
        assert "HIGH LIQUIDATION RISK" in result
        assert "Bybit" in result
        assert "65.5%" in result

    def test_format_high_margin_ratio_alert_none_ratio(self):
        state = _make_state(ratio=None)
        result = self.fmt.format_high_margin_ratio_alert(state)
        assert "N/A" in result

    # --- format_stale_data_alert ---

    def test_format_stale_data_alert(self):
        now = datetime.now(tz=timezone.utc)
        last = datetime(2026, 3, 11, 10, 0, 0, tzinfo=timezone.utc)
        result = self.fmt.format_stale_data_alert("Binance", "positions", last, now)
        assert "Binance" in result
        assert "STALE DATA" in result

    # --- format_position_reduction_batch ---

    def test_format_position_reduction_batch_single(self):
        batch = [{
            "exchange_name": "Bybit",
            "ticker": "SOLUSDT",
            "old_amount": Decimal("10"),
            "new_amount": Decimal("5"),
            "counterpart": None,
        }]
        result = self.fmt.format_position_reduction_batch(batch)
        assert "SOLUSDT" in result
        assert "Bybit" in result
        assert "1 event" in result

    def test_format_position_reduction_batch_multi(self):
        batch = [
            {"exchange_name": "Bybit", "ticker": "BTCUSDT",
             "old_amount": Decimal("1"), "new_amount": Decimal("0"), "counterpart": None},
            {"exchange_name": "Bybit", "ticker": "ETHUSDT",
             "old_amount": Decimal("5"), "new_amount": Decimal("3"), "counterpart": None},
        ]
        result = self.fmt.format_position_reduction_batch(batch)
        assert "2 event" in result
        assert "BTCUSDT" in result
        assert "ETHUSDT" in result

    def test_format_position_reduction_batch_fully_closed_marked(self):
        batch = [{"exchange_name": "X", "ticker": "T", "old_amount": Decimal("1"),
                  "new_amount": Decimal("0"), "counterpart": None}]
        result = self.fmt.format_position_reduction_batch(batch)
        assert "Fully closed" in result or "fully closed" in result.lower()

    # --- format_session_start_separator ---

    def test_format_session_start_separator(self):
        result = self.fmt.format_session_start_separator()
        assert "NEW SESSION" in result
        assert len(result) > 10

    # --- format_exchange_state ---

    def test_format_exchange_state_no_positions(self):
        state = _make_state(name="Bybit", positions={})
        result = self.fmt.format_exchange_state(state)
        assert "Bybit" in result
        assert "No open positions" in result
        assert "Margin" in result

    def test_format_exchange_state_with_positions(self):
        pos = _make_position("BTCUSDT", "long", "0.1", "60000", "62000")
        state = _make_state(name="Bybit", positions={"BTCUSDT": pos})
        result = self.fmt.format_exchange_state(state)
        assert "BTCUSDT" in result
        # direction отображается через emoji (🟢/🔴), а не текстом LONG/SHORT
        assert "🟢" in result

    def test_format_exchange_state_short_position_pnl_sign(self):
        """Для шорта при росте цены PnL отрицательный."""
        pos = _make_position("BTCUSDT", "short", "0.1", "60000", "62000")
        state = _make_state(positions={"BTCUSDT": pos})
        result = self.fmt.format_exchange_state(state)
        # при short + цена выросла: должен быть минусовой PnL
        assert "-" in result

    def test_format_exchange_state_updated_line(self):
        state = _make_state()
        result = self.fmt.format_exchange_state(state)
        assert "Updated:" in result

    # --- внутренние хелперы ---

    def test_fmt_price_strips_trailing_zeros(self):
        from app.telegram.formatters import _fmt_price
        result = _fmt_price(Decimal("60000.00000000"))
        assert result.endswith("00")  # минимум 2 знака после точки
        assert "000000" not in result  # лишних нулей нет

    def test_fmt_price_keeps_significant_decimals(self):
        from app.telegram.formatters import _fmt_price
        result = _fmt_price(Decimal("0.00012345"))
        assert "12345" in result

    def test_fmt_num_large_number(self):
        from app.telegram.formatters import _fmt_num
        result = _fmt_num(Decimal("1234567.89"))
        # разделитель тысяч — пробел
        assert " " in result
        assert "1" in result


# ===========================================================================
# Тест 3 — Timezone
# ===========================================================================

class TestTimezone:

    def _get_tz(self, tz_str: str):
        """Вызывает get_notify_tz() с заданным значением NOTIFY_TIMEZONE."""
        import app.connectors.config as cfg
        old = cfg.NOTIFY_TIMEZONE
        cfg.NOTIFY_TIMEZONE = tz_str
        try:
            return cfg.get_notify_tz()
        finally:
            cfg.NOTIFY_TIMEZONE = old

    def test_utc_returns_utc(self):
        from datetime import timezone as _tz
        result = self._get_tz("UTC")
        assert result == _tz.utc

    def test_utc_lowercase(self):
        from datetime import timezone as _tz
        result = self._get_tz("utc")
        assert result == _tz.utc

    def test_empty_string_returns_utc(self):
        from datetime import timezone as _tz
        result = self._get_tz("")
        assert result == _tz.utc

    def test_europe_moscow(self):
        import zoneinfo
        result = self._get_tz("Europe/Moscow")
        assert isinstance(result, zoneinfo.ZoneInfo)
        assert str(result) == "Europe/Moscow"

    def test_asia_tokyo(self):
        import zoneinfo
        result = self._get_tz("Asia/Tokyo")
        assert isinstance(result, zoneinfo.ZoneInfo)
        assert str(result) == "Asia/Tokyo"

    def test_america_new_york(self):
        import zoneinfo
        result = self._get_tz("America/New_York")
        assert isinstance(result, zoneinfo.ZoneInfo)

    def test_invalid_tz_falls_back_to_utc(self):
        from datetime import timezone as _tz
        result = self._get_tz("Invalid/Zone_XYZ_999")
        assert result == _tz.utc

    def test_to_notify_tz_naive_datetime(self):
        """Naive datetime должен трактоваться как UTC и корректно конвертироваться."""
        import app.connectors.config as cfg
        old = cfg.NOTIFY_TIMEZONE
        cfg.NOTIFY_TIMEZONE = "Europe/Moscow"
        try:
            from app.telegram.formatters import _to_notify_tz
            naive_dt = datetime(2026, 3, 11, 12, 0, 0)  # naive
            result = _to_notify_tz(naive_dt)
            # UTC 12:00 -> Moscow +3 = 15:00
            assert result.hour == 15
        finally:
            cfg.NOTIFY_TIMEZONE = old

    def test_to_notify_tz_aware_datetime(self):
        """Aware datetime конвертируется без потери информации."""
        import app.connectors.config as cfg
        old = cfg.NOTIFY_TIMEZONE
        cfg.NOTIFY_TIMEZONE = "Asia/Tokyo"
        try:
            from app.telegram.formatters import _to_notify_tz
            aware_dt = datetime(2026, 3, 11, 0, 0, 0, tzinfo=timezone.utc)
            result = _to_notify_tz(aware_dt)
            # UTC 00:00 -> Tokyo +9 = 09:00
            assert result.hour == 9
        finally:
            cfg.NOTIFY_TIMEZONE = old


# ===========================================================================
# Тест 4 — TelegramAlertService: инициализация и get_updates
# ===========================================================================

class TestTelegramService:

    def test_init_raises_on_empty_token(self):
        from app.telegram.service import TelegramAlertService
        with pytest.raises(ValueError, match="bot_token"):
            TelegramAlertService("")

    def test_init_ok_with_dummy_token(self):
        from app.telegram.service import TelegramAlertService
        svc = TelegramAlertService("dummy:token123")
        assert svc._base == "https://api.telegram.org/botdummy:token123"
        assert svc._client is None  # не запущен

    def test_get_updates_returns_empty_when_not_started(self):
        """Без вызова start() — get_updates должен вернуть [] без исключения."""
        from app.telegram.service import TelegramAlertService
        svc = TelegramAlertService("dummy:token123")

        async def _run():
            return await svc.get_updates()

        result = asyncio.get_event_loop().run_until_complete(_run())
        assert result == []

    def test_start_creates_http_client(self):
        """После start() клиент инициализирован."""
        from app.telegram.service import TelegramAlertService
        svc = TelegramAlertService("dummy:token123")

        async def _run():
            await svc.start()
            ok = svc._client is not None
            await svc.stop()
            return ok

        result = asyncio.get_event_loop().run_until_complete(_run())
        assert result is True

    def test_stop_clears_client(self):
        """После stop() клиент None."""
        from app.telegram.service import TelegramAlertService
        svc = TelegramAlertService("dummy:token123")

        async def _run():
            await svc.start()
            await svc.stop()
            return svc._client

        result = asyncio.get_event_loop().run_until_complete(_run())
        assert result is None

    def test_get_updates_with_invalid_token_returns_empty(self):
        """Реальный HTTP-запрос с фейковым токеном: должен поймать ошибку и вернуть []."""
        from app.telegram.service import TelegramAlertService
        svc = TelegramAlertService("000000000:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

        async def _run():
            await svc.start()
            t0 = time.monotonic()
            result = await svc.get_updates()
            latency_ms = int((time.monotonic() - t0) * 1000)
            await svc.stop()
            return result, latency_ms

        updates, latency = asyncio.get_event_loop().run_until_complete(_run())
        # Telegram вернёт {"ok": false} — сервис должен вернуть []
        assert updates == []
        # latency фиксируем для отчёта (не assertion)
        assert latency >= 0
