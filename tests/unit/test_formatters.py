"""
Тесты для format_all_states_brief (app/telegram/formatters.py).
"""
from decimal import Decimal

from app.telegram.formatters import format_all_states_brief
from tests.unit.conftest import make_position, make_state


class TestFormatAllStatesBrief:

    def test_contains_exchange_name(self):
        states = {"binance": make_state("binance")}
        result = format_all_states_brief(states)
        assert "BINANCE" in result

    def test_contains_all_exchange_names(self):
        states = {
            "binance": make_state("binance"),
            "bybit": make_state("bybit"),
        }
        result = format_all_states_brief(states)
        assert "BINANCE" in result
        assert "BYBIT" in result

    def test_contains_monitor_header(self):
        result = format_all_states_brief({})
        assert "Monitor" in result

    def test_contains_margin_ratio(self):
        state = make_state("binance")
        state.maintenance_margin = Decimal("1000")
        state.current_margin = Decimal("200")
        # margin_ratio считается в base.py — установим напрямую
        state.margin_ratio = Decimal("20")
        result = format_all_states_brief({"binance": state})
        assert "20.0%" in result

    def test_contains_current_margin(self):
        state = make_state("binance")
        state.current_margin = Decimal("1234.56")
        state.maintenance_margin = Decimal("500")
        result = format_all_states_brief({"binance": state})
        assert "1 234.56" in result

    def test_contains_maintenance_margin(self):
        state = make_state("binance")
        state.current_margin = Decimal("1000")
        state.maintenance_margin = Decimal("567.89")
        result = format_all_states_brief({"binance": state})
        assert "567.89" in result

    def test_position_count_shown(self):
        state = make_state("binance")
        state.positions = {
            "BTCUSDT": make_position("BTCUSDT", "binance"),
            "ETHUSDT": make_position("ETHUSDT", "binance"),
        }
        result = format_all_states_brief({"binance": state})
        assert "2 pos" in result

    def test_zero_positions(self):
        state = make_state("binance")
        result = format_all_states_brief({"binance": state})
        assert "0 pos" in result

    def test_empty_states_returns_only_header(self):
        result = format_all_states_brief({})
        assert "Monitor" in result
        # нет строк с биржами
        assert "pos" not in result

    def test_low_ratio_green_emoji(self):
        state = make_state("binance")
        state.margin_ratio = Decimal("10")
        result = format_all_states_brief({"binance": state})
        assert "🟢" in result

    def test_medium_ratio_yellow_emoji(self):
        state = make_state("binance")
        state.margin_ratio = Decimal("40")
        result = format_all_states_brief({"binance": state})
        assert "🟡" in result

    def test_high_ratio_red_emoji(self):
        state = make_state("binance")
        state.margin_ratio = Decimal("60")
        result = format_all_states_brief({"binance": state})
        assert "🔴" in result
