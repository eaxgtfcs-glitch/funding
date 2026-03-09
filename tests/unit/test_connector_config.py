"""
Тесты для ConnectorConfig (app/connectors/config.py).
"""
import pytest

from app.connectors.config import ConnectorConfig, DEFAULT_CONFIG


class TestConnectorConfigDefaults:

    def test_positions_interval_default_value(self):
        config = ConnectorConfig()
        assert config.positions_interval == 5

    def test_margin_interval_default_value(self):
        config = ConnectorConfig()
        assert config.margin_interval == 5

    def test_funding_interval_default_value(self):
        config = ConnectorConfig()
        assert config.funding_interval == 5

    def test_default_config_singleton_positions_interval(self):
        assert DEFAULT_CONFIG.positions_interval == 5

    def test_default_config_singleton_margin_interval(self):
        assert DEFAULT_CONFIG.margin_interval == 5

    def test_default_config_singleton_funding_interval(self):
        assert DEFAULT_CONFIG.funding_interval == 5


class TestConnectorConfigCustomValues:

    def test_custom_positions_interval(self):
        config = ConnectorConfig(positions_interval=5.0)
        assert config.positions_interval == 5.0

    def test_custom_margin_interval(self):
        config = ConnectorConfig(margin_interval=15.0)
        assert config.margin_interval == 15.0

    def test_custom_funding_interval(self):
        config = ConnectorConfig(funding_interval=120.0)
        assert config.funding_interval == 120.0

    def test_all_custom_values(self):
        config = ConnectorConfig(
            positions_interval=1.0,
            margin_interval=2.0,
            funding_interval=3.0,
        )
        assert config.positions_interval == 1.0
        assert config.margin_interval == 2.0
        assert config.funding_interval == 3.0

    def test_custom_config_does_not_affect_default_config(self):
        ConnectorConfig(positions_interval=999)
        assert DEFAULT_CONFIG.positions_interval == 5

    @pytest.mark.parametrize("interval", [0, 1, 60, 3600])
    def test_positions_interval_boundary_values(self, interval: int):
        config = ConnectorConfig(positions_interval=interval)
        assert config.positions_interval == interval
