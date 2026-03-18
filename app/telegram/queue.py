import asyncio
import logging
from typing import Callable, Coroutine

logger = logging.getLogger(__name__)


class TelegramQueue:
    CRITICAL = 0
    ALERT = 1
    STATE = 2

    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._counter = 0
        self._task: asyncio.Task | None = None

    def enqueue(self, priority: int, coro_fn: Callable[[], Coroutine]) -> None:
        self._queue.put_nowait((priority, self._counter, coro_fn))
        self._counter += 1

    async def start(self) -> None:
        self._task = asyncio.create_task(self._worker())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _worker(self) -> None:
        while True:
            _, _, coro_fn = await self._queue.get()
            try:
                await coro_fn()
            except Exception:
                logger.exception("TelegramQueue: error in coro_fn")
            finally:
                self._queue.task_done()
            await asyncio.sleep(1)
