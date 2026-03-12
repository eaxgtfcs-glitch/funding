"""
Тесты для MonitoringEngine (app/engine/engine.py).

MonitoringEngine при инициализации вызывает _discover_connectors, который:
  1. Сканирует пакет app.connectors через pkgutil.iter_modules
  2. Импортирует найденные модули (в т.ч. bybit.py, читающий os.environ)
  3. Инстанциирует все неабстрактные подклассы BaseExchangeConnector

Чтобы изолировать тесты от реальных коннекторов и переменных окружения,
патчим pkgutil.iter_modules (возвращаем пустой список) и предоставляем
тестовый дубль через BaseExchangeConnector.__subclasses__.
"""
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from app.connectors.base import BaseExchangeConnector
from app.connectors.config import ConnectorConfig
from app.connectors.model.position import Position
from app.connectors.model.state import ExchangeState


# ---------------------------------------------------------------------------
# Тестовый дубль коннектора для движка
# ---------------------------------------------------------------------------

class StubConnector(BaseExchangeConnector):
    """Заглушка коннектора, не требующая API-ключей и не обращающаяся к сети."""

    name = "stub"
    config = ConnectorConfig(
        positions_interval=0.0,
        margin_interval=0.0,
    )

    async def fetch_positions(self) -> list[Position]:
        return []

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        return Decimal(0), Decimal(0)


class AnotherStubConnector(BaseExchangeConnector):
    """Второй дубль для проверки работы с несколькими коннекторами."""

    name = "another_stub"
    config = ConnectorConfig(
        positions_interval=0.0,
        margin_interval=0.0,
    )

    async def fetch_positions(self) -> list[Position]:
        return []

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        return Decimal(0), Decimal(0)


# ---------------------------------------------------------------------------
# Вспомогательная фабрика движка с изолированными коннекторами
# ---------------------------------------------------------------------------

def make_engine_with_connectors(connector_classes: list):
    """
    Создаёт MonitoringEngine, у которого _discover_connectors инстанциирует
    только переданные connector_classes (без сканирования файловой системы
    и без импорта bybit.py).
    """
    from app.engine.engine import MonitoringEngine

    # Подавляем iter_modules, чтобы не импортировались реальные модули
    with patch("pkgutil.iter_modules", return_value=[]):
        # Подменяем __subclasses__, чтобы движок видел только наши заглушки
        with patch.object(
                BaseExchangeConnector,
                "__subclasses__",
                return_value=connector_classes,
        ):
            engine = MonitoringEngine()
    return engine


# ===========================================================================
# Тесты: _discover_connectors / инициализация
# ===========================================================================

class TestMonitoringEngineDiscovery:

    def test_states_contains_connector_name_after_discovery(self):
        engine = make_engine_with_connectors([StubConnector])
        assert "stub" in engine.states

    def test_states_references_connector_state_object(self):
        engine = make_engine_with_connectors([StubConnector])
        connector = engine._connectors[0]
        assert engine.states["stub"] is connector.state

    def test_states_contains_all_discovered_connectors(self):
        engine = make_engine_with_connectors([StubConnector, AnotherStubConnector])
        assert "stub" in engine.states
        assert "another_stub" in engine.states

    def test_connectors_list_has_correct_length(self):
        engine = make_engine_with_connectors([StubConnector, AnotherStubConnector])
        assert len(engine._connectors) == 2

    def test_empty_connector_list_gives_empty_states(self):
        engine = make_engine_with_connectors([])
        assert engine.states == {}
        assert engine._connectors == []

    def test_abstract_connector_is_not_instantiated(self):
        """Абстрактный класс не должен попасть в _connectors."""

        # BaseExchangeConnector сам является абстрактным — передаём его
        # вместе с реальным, чтобы убедиться, что движок его пропускает
        engine = make_engine_with_connectors([BaseExchangeConnector, StubConnector])
        connector_names = [c.name for c in engine._connectors]
        assert "stub" in connector_names
        # BaseExchangeConnector не имеет атрибута name как строки —
        # главное, что он не вошёл в список как инстанс
        assert len(engine._connectors) == 1

    def test_state_is_instance_of_exchange_state(self):
        engine = make_engine_with_connectors([StubConnector])
        assert isinstance(engine.states["stub"], ExchangeState)


# ===========================================================================
# Тесты: start()
# ===========================================================================

class TestMonitoringEngineStart:

    async def test_start_calls_start_on_each_connector(self):
        engine = make_engine_with_connectors([StubConnector, AnotherStubConnector])

        # Подменяем start у каждого коннектора на AsyncMock
        for connector in engine._connectors:
            connector.start = AsyncMock()

        await engine.start()

        for connector in engine._connectors:
            connector.start.assert_awaited_once()

    async def test_start_calls_start_on_single_connector(self):
        engine = make_engine_with_connectors([StubConnector])
        connector = engine._connectors[0]
        connector.start = AsyncMock()

        await engine.start()

        connector.start.assert_awaited_once()

    async def test_start_with_no_connectors_does_not_raise(self):
        engine = make_engine_with_connectors([])

        # Не должно бросать исключений
        await engine.start()

    async def test_start_creates_background_tasks(self):
        """start() у базового класса создаёт реальные задачи — проверяем интеграцию."""
        engine = make_engine_with_connectors([StubConnector])
        connector = engine._connectors[0]

        await engine.start()

        try:
            assert len(connector._tasks) == 2
        finally:
            await engine.stop()


# ===========================================================================
# Тесты: stop()
# ===========================================================================

class TestMonitoringEngineStop:

    async def test_stop_calls_stop_on_each_connector(self):
        engine = make_engine_with_connectors([StubConnector, AnotherStubConnector])

        for connector in engine._connectors:
            connector.stop = AsyncMock()

        await engine.stop()

        for connector in engine._connectors:
            connector.stop.assert_awaited_once()

    async def test_stop_calls_stop_on_single_connector(self):
        engine = make_engine_with_connectors([StubConnector])
        connector = engine._connectors[0]
        connector.stop = AsyncMock()

        await engine.stop()

        connector.stop.assert_awaited_once()

    async def test_stop_with_no_connectors_does_not_raise(self):
        engine = make_engine_with_connectors([])

        await engine.stop()

    async def test_stop_cancels_running_tasks(self):
        """После stop() фоновые задачи коннектора должны быть отменены."""
        engine = make_engine_with_connectors([StubConnector])
        connector = engine._connectors[0]

        await engine.start()
        assert len(connector._tasks) == 2

        await engine.stop()

        # После stop задачи должны быть очищены
        assert connector._tasks == []

    async def test_start_then_stop_cycle_does_not_raise(self):
        """Полный цикл start → stop не должен порождать исключений."""
        engine = make_engine_with_connectors([StubConnector, AnotherStubConnector])

        await engine.start()
        await engine.stop()


# ===========================================================================
# Тесты: states ссылается на state коннекторов
# ===========================================================================

class TestMonitoringEngineStates:

    def test_states_dict_value_is_same_object_as_connector_state(self):
        """engine.states[name] — это тот же объект, что connector.state."""
        engine = make_engine_with_connectors([StubConnector])
        connector = engine._connectors[0]

        assert engine.states[connector.name] is connector.state

    def test_mutation_of_connector_state_visible_via_engine_states(self):
        """Изменение connector.state отражается в engine.states (shared reference)."""
        engine = make_engine_with_connectors([StubConnector])
        connector = engine._connectors[0]

        connector.state.current_margin = Decimal("12345")

        assert engine.states["stub"].current_margin == Decimal("12345")

    def test_states_for_two_connectors_are_independent_objects(self):
        engine = make_engine_with_connectors([StubConnector, AnotherStubConnector])
        stub_state = engine.states["stub"]
        another_state = engine.states["another_stub"]

        assert stub_state is not another_state
