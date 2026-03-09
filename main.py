import asyncio
import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


async def main() -> None:
    from app.engine.engine import MonitoringEngine
    engine = MonitoringEngine()
    await engine.start()
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        await engine.stop()


if __name__ == "__main__":
    asyncio.run(main())
