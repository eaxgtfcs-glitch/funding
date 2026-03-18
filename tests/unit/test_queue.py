"""
Тесты для TelegramQueue (app/telegram/queue.py).

Покрываемые сценарии:
- Порядок приоритетов: CRITICAL → ALERT → STATE
- Tiebreaker по счётчику (FIFO внутри одного приоритета)
- Задержка asyncio.sleep(1) между вызовами воркера
- Исключение в coro_fn не роняет воркера
- stop() останавливает воркера, новые элементы не обрабатываются
- enqueue без start не падает
"""
import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from app.telegram.queue import TelegramQueue


# ---------------------------------------------------------------------------
# Вспомогательные утилиты
# ---------------------------------------------------------------------------

def _make_recording_coro(log: list, value: object):
    """Возвращает фабрику корутины, которая дописывает value в log."""

    async def _coro():
        log.append(value)

    return _coro


# ---------------------------------------------------------------------------
# Тест 1: порядок приоритетов CRITICAL → ALERT → STATE
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_priority_order_critical_before_alert_before_state():
    """Независимо от порядка enqueue воркер обрабатывает CRITICAL → ALERT → STATE."""
    q = TelegramQueue()

    # Блокируем sleep, чтобы не ждать реальную секунду
    with patch("app.telegram.queue.asyncio.sleep", new=AsyncMock()):
        await q.start()

        executed: list[str] = []

        # Добавляем в «неправильном» порядке
        q.enqueue(TelegramQueue.STATE, _make_recording_coro(executed, "STATE"))
        q.enqueue(TelegramQueue.ALERT, _make_recording_coro(executed, "ALERT"))
        q.enqueue(TelegramQueue.CRITICAL, _make_recording_coro(executed, "CRITICAL"))

        # Ждём, пока очередь опустеет
        await q._queue.join()
        await q.stop()

    assert executed == ["CRITICAL", "ALERT", "STATE"]


# ---------------------------------------------------------------------------
# Тест 2: tiebreaker — FIFO внутри одного приоритета
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tiebreaker_fifo_within_same_priority():
    """Два CRITICAL-элемента обрабатываются в порядке добавления."""
    q = TelegramQueue()

    with patch("app.telegram.queue.asyncio.sleep", new=AsyncMock()):
        await q.start()

        executed: list[int] = []

        q.enqueue(TelegramQueue.CRITICAL, _make_recording_coro(executed, 1))
        q.enqueue(TelegramQueue.CRITICAL, _make_recording_coro(executed, 2))
        q.enqueue(TelegramQueue.CRITICAL, _make_recording_coro(executed, 3))

        await q._queue.join()
        await q.stop()

    assert executed == [1, 2, 3]


# ---------------------------------------------------------------------------
# Тест 3: задержка между вызовами
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_worker_sleeps_between_calls():
    """Воркер вызывает asyncio.sleep(1) после каждого элемента."""
    q = TelegramQueue()

    sleep_calls: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    with patch("app.telegram.queue.asyncio.sleep", new=_fake_sleep):
        await q.start()

        executed: list[int] = []
        q.enqueue(TelegramQueue.ALERT, _make_recording_coro(executed, 1))
        q.enqueue(TelegramQueue.ALERT, _make_recording_coro(executed, 2))

        await q._queue.join()
        await q.stop()

    # sleep вызывается после каждого обработанного элемента
    assert len(sleep_calls) >= 2
    assert all(d == 1 for d in sleep_calls)


# ---------------------------------------------------------------------------
# Тест 4: исключение в coro_fn не роняет воркера
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_exception_in_coro_fn_does_not_kill_worker():
    """Если coro_fn бросает Exception, воркер продолжает работать."""
    q = TelegramQueue()

    with patch("app.telegram.queue.asyncio.sleep", new=AsyncMock()):
        await q.start()

        executed: list[str] = []

        async def _failing_coro():
            raise RuntimeError("intentional error")

        q.enqueue(TelegramQueue.CRITICAL, _failing_coro)
        q.enqueue(TelegramQueue.CRITICAL, _make_recording_coro(executed, "after_error"))

        await q._queue.join()
        await q.stop()

    # Второй элемент должен быть обработан несмотря на ошибку в первом
    assert executed == ["after_error"]


# ---------------------------------------------------------------------------
# Тест 5: task_done вызывается даже при CancelledError в coro_fn
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_task_done_called_on_cancelled_error():
    """
    Если coro_fn поднимает CancelledError (не является подклассом Exception),
    finally в _worker всё равно вызывает task_done(), и очередь не зависает.
    """
    q = TelegramQueue()

    barrier = asyncio.Event()

    async def _slow_coro():
        # Ждём сигнала, затем выбрасываем CancelledError
        await barrier.wait()
        raise asyncio.CancelledError()

    with patch("app.telegram.queue.asyncio.sleep", new=AsyncMock()):
        await q.start()

        q.enqueue(TelegramQueue.CRITICAL, _slow_coro)

        # Даём воркеру взять элемент из очереди
        await asyncio.sleep(0)
        # Разблокируем coro, которая бросит CancelledError
        barrier.set()

        # join должен завершиться без зависания — task_done() был вызван в finally
        try:
            await asyncio.wait_for(q._queue.join(), timeout=2.0)
        except asyncio.TimeoutError:
            pytest.fail("queue.join() завис — task_done() не был вызван при CancelledError")
        finally:
            await q.stop()


# ---------------------------------------------------------------------------
# Тест 6: stop() останавливает воркера
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_prevents_processing_new_items():
    """После stop() элементы, добавленные в очередь, не обрабатываются."""
    q = TelegramQueue()

    with patch("app.telegram.queue.asyncio.sleep", new=AsyncMock()):
        await q.start()
        await q.stop()

    executed: list[str] = []
    q.enqueue(TelegramQueue.ALERT, _make_recording_coro(executed, "should_not_run"))

    # Даём потенциальному воркеру время выполниться (воркера нет)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert executed == [], "Воркер не должен обрабатывать элементы после stop()"


# ---------------------------------------------------------------------------
# Тест 7: stop() после повторного вызова не падает
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_is_idempotent():
    """Повторный вызов stop() не вызывает исключений."""
    q = TelegramQueue()
    with patch("app.telegram.queue.asyncio.sleep", new=AsyncMock()):
        await q.start()
        await q.stop()
        await q.stop()  # второй вызов не должен падать


# ---------------------------------------------------------------------------
# Тест 8: enqueue без start не падает
# ---------------------------------------------------------------------------

def test_enqueue_without_start_does_not_raise():
    """put_nowait работает без запущенного воркера — элемент попадает в очередь."""
    q = TelegramQueue()

    executed: list[str] = []
    # Не должно бросать исключений
    q.enqueue(TelegramQueue.STATE, _make_recording_coro(executed, "queued"))

    # Элемент лежит в очереди
    assert q._queue.qsize() == 1


# ---------------------------------------------------------------------------
# Тест 9: счётчик tiebreaker монотонно возрастает
# ---------------------------------------------------------------------------

def test_counter_increments_on_each_enqueue():
    """_counter увеличивается при каждом enqueue, обеспечивая FIFO внутри приоритета."""
    q = TelegramQueue()

    async def _noop():
        pass

    assert q._counter == 0
    q.enqueue(TelegramQueue.ALERT, _noop)
    assert q._counter == 1
    q.enqueue(TelegramQueue.CRITICAL, _noop)
    assert q._counter == 2


# ---------------------------------------------------------------------------
# Тест 10: смешанный порядок — два ALERT между двумя CRITICAL
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mixed_priorities_correct_global_order():
    """
    Enqueue: ALERT(1), CRITICAL(2), STATE(3), CRITICAL(4)
    Ожидаемый порядок обработки: CRITICAL(2), CRITICAL(4), ALERT(1), STATE(3)
    """
    q = TelegramQueue()

    with patch("app.telegram.queue.asyncio.sleep", new=AsyncMock()):
        await q.start()

        executed: list[str] = []

        q.enqueue(TelegramQueue.ALERT, _make_recording_coro(executed, "alert_1"))
        q.enqueue(TelegramQueue.CRITICAL, _make_recording_coro(executed, "critical_2"))
        q.enqueue(TelegramQueue.STATE, _make_recording_coro(executed, "state_3"))
        q.enqueue(TelegramQueue.CRITICAL, _make_recording_coro(executed, "critical_4"))

        await q._queue.join()
        await q.stop()

    assert executed == ["critical_2", "critical_4", "alert_1", "state_3"]
