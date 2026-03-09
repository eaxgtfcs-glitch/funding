from datetime import datetime
from decimal import Decimal

from app.connectors.model.position import Position
from app.connectors.model.state import ExchangeState


def format_margin_alert(state: ExchangeState, threshold_pct: Decimal) -> str:
    # считаем остаток маржи в процентах; защита от деления на ноль
    pct_remaining = (
        (state.current_margin / state.maintenance_margin * 100)
        if state.maintenance_margin > 0
        else Decimal(0)
    )
    return (
        f"<b>MARGIN ALERT</b> — {state.name}\n"
        f"Maintenance margin: <code>{state.maintenance_margin:.2f}</code>\n"
        f"Current margin:     <code>{state.current_margin:.2f}</code>\n"
        f"Remaining:          <code>{pct_remaining:.1f}%</code> (threshold {threshold_pct * 100:.0f}%)\n"
        f"Time: {state.maintenance_margin_update_time.isoformat(timespec='seconds')}"
    )


def format_position_reduction_alert(
        exchange_name: str,
        ticker: str,
        old_amount: Decimal,
        new_amount: Decimal,
        counterpart: Position | None,
) -> str:
    lines = [
        f"<b>POSITION REDUCED</b> — {exchange_name}",
        f"Ticker: <code>{ticker}</code>",
        f"Amount: <code>{old_amount}</code> → <code>{new_amount}</code>",
    ]
    # отдельно помечаем полное закрытие — вероятная ликвидация
    if new_amount == Decimal(0):
        lines.append("<b>Position fully closed (liquidation/ADL?)</b>")

    # если известна парная позиция — добавляем её детали в алерт
    if counterpart:
        lines.append("")
        lines.append("<b>Paired position (action may be needed):</b>")
        lines.append(
            f"  Exchange: {counterpart.exchange_name}  Ticker: {counterpart.ticker}"
        )
        lines.append(
            f"  Direction: {counterpart.direction}  Amount: {counterpart.amount}"
        )
        lines.append(
            f"  Avg price: {counterpart.avg_price}  Current: {counterpart.current_price}"
        )

    return "\n".join(lines)


def format_stale_connector_alert(exchange_name: str, last_update: datetime, now: datetime) -> str:
    """last_update and now may be tz-aware or naive — both must be the same kind."""
    delta_seconds = int((now - last_update).total_seconds())
    ts = last_update.isoformat(timespec="seconds")
    return (
        f"<b>STALE CONNECTOR</b> — {exchange_name}\n"
        f"No update received for <code>{delta_seconds}s</code>.\n"
        f"Last update: {ts}"
    )


def _fmt_num(value: Decimal) -> str:
    # space-separated thousands, 2 decimal places
    return f"{float(value):,.2f}".replace(",", " ")


def _fmt_price(value: Decimal) -> str:
    # space-separated thousands, up to 8 decimal places, trailing zeros stripped (min 2)
    raw = f"{float(value):,.8f}".replace(",", " ")
    integer_part, decimal_part = raw.split(".")
    decimal_part = decimal_part.rstrip("0")
    if len(decimal_part) < 2:
        decimal_part = decimal_part.ljust(2, "0")
    return f"{integer_part}.{decimal_part}"


def format_exchange_state(state: ExchangeState) -> str:
    lines: list[str] = []

    # Header
    lines.append(f"<b>{state.name.upper()}  |  ONLINE</b>")
    lines.append("")

    # Margin
    lines.append("<b>Margin</b>")
    if state.maintenance_margin > 0:
        margin_pct = state.current_margin / state.maintenance_margin * 100
        lines.append(f"  Current:  <code>{_fmt_num(state.current_margin)} USDT</code>")
        lines.append(
            f"  Required: <code>{_fmt_num(state.maintenance_margin)} USDT</code>"
            f"  ({float(margin_pct):.1f}%)"
        )
    else:
        lines.append(f"  Current:  <code>{_fmt_num(state.current_margin)} USDT</code>")
        lines.append(f"  Required: <code>{_fmt_num(state.maintenance_margin)} USDT</code>")
    lines.append("")

    # Positions
    positions = list(state.positions.values())
    lines.append(f"<b>Positions ({len(positions)})</b>")
    if not positions:
        lines.append("  No open positions")
    else:
        for pos in positions:
            if pos.avg_price > 0:
                raw_pnl = (pos.current_price - pos.avg_price) / pos.avg_price * 100
                # для лонга прибыль при росте цены, для шорта — при падении
                pnl = raw_pnl if pos.direction == "long" else -raw_pnl
            else:
                pnl = Decimal(0)

            # 📈 когда позиция в плюсе (независимо от направления), 📉 когда в минусе
            trend_emoji = "\U0001f4c8" if pnl >= 0 else "\U0001f4c9"
            sign = "+" if pnl >= 0 else ""
            lines.append(
                f"  <code>{pos.ticker:<10}</code> {pos.direction.upper():<5} "
                f"<code>{pos.amount}</code>  "
                f"avg <code>{_fmt_price(pos.avg_price)}</code>  "
                f"now <code>{_fmt_price(pos.current_price)}</code>  "
                f"{trend_emoji} {sign}{float(pnl):.2f}%"
            )
    lines.append("")

    # Funding rates (только текущие, без истории)
    funding = state.funding_rates
    lines.append("<b>Funding Rates</b>")
    if not funding:
        lines.append("  No data")
    else:
        for ticker, snapshot in funding.items():
            rate_pct = snapshot.rate * 100
            sign = "+" if snapshot.rate >= 0 else ""
            lines.append(f"  <code>{ticker:<10}</code> {sign}{float(rate_pct):.4f}%")
    lines.append("")

    # Updated — берём наибольшее из трёх времён
    latest = max(
        state.positions_update_time,
        state.maintenance_margin_update_time,
        state.funding_rates_update_time,
    )
    lines.append(f"Updated: {latest.strftime('%H:%M:%S.%f')[:-3]} UTC")

    return "\n".join(lines)
