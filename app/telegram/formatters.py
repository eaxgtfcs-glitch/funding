from __future__ import annotations

from datetime import datetime
from datetime import timezone as _tz
from decimal import Decimal
from typing import TYPE_CHECKING

from app.connectors.config import get_notify_tz
from app.connectors.model.position import Position
from app.connectors.model.state import ExchangeState

if TYPE_CHECKING:
    from app.engine.model.structure import Structure


def _to_notify_tz(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_tz.utc)
    return dt.astimezone(get_notify_tz())


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
        f"Time: {_to_notify_tz(state.maintenance_margin_update_time).strftime('%Y-%m-%dT%H:%M:%S %Z')}"
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
        lines.append("<b>Position fully closed</b>")

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
    ts = _to_notify_tz(last_update).strftime("%Y-%m-%dT%H:%M:%S %Z")
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


def _fmt_margin_ratio(ratio: Decimal | None) -> tuple[str, str]:
    """Returns (emoji, text) where text is e.g. '12.3%'."""
    if ratio is None:
        return "⚪", f"{0.0:.1f}%"
    v = float(ratio)
    if v <= 30:
        emoji = "🟢"
    elif v <= 50:
        emoji = "🟡"
    else:
        emoji = "🔴"
    return emoji, f"{v:.1f}%"


def format_high_margin_ratio_alert(state: ExchangeState) -> str:
    ratio = state.margin_ratio
    ratio_str = f"{float(ratio):.1f}%" if ratio is not None else "N/A"
    return (
        f"<b>HIGH LIQUIDATION RISK</b> — {state.name}\n"
        f"Margin ratio: <code>{ratio_str}</code>  (maintenance / current)\n"
        f"Maintenance: <code>{_fmt_num(state.maintenance_margin)} USDT</code>\n"
        f"Current:     <code>{_fmt_num(state.current_margin)} USDT</code>\n"
        f"Time: {_to_notify_tz(state.maintenance_margin_update_time).strftime('%Y-%m-%dT%H:%M:%S %Z')}"
    )


def format_stale_data_alert(exchange_name: str, field_name: str, last_update: datetime, now: datetime) -> str:
    delta_seconds = int((now - last_update).total_seconds())
    ts = _to_notify_tz(last_update).strftime("%Y-%m-%dT%H:%M:%S %Z")
    return (
        f"<b>STALE DATA</b> — {exchange_name}\n"
        f"<code>{field_name}</code> not updated for <code>{delta_seconds}s</code>.\n"
        f"Last update: {ts}"
    )


def format_position_reduction_batch(reductions: list[dict]) -> str:
    """reductions — список словарей с ключами: exchange_name, ticker, old_amount, new_amount, counterpart."""
    lines = [f"<b>POSITION REDUCED</b> ({len(reductions)} event(s))"]
    for r in reductions:
        lines.append("")
        lines.append(f"<b>{r['exchange_name']}</b>  <code>{r['ticker']}</code>")
        lines.append(f"  Amount: <code>{r['old_amount']}</code> → <code>{r['new_amount']}</code>")
        if r["new_amount"] == Decimal(0):
            lines.append("  <b>Fully closed</b>")
        counterpart = r.get("counterpart")
        if counterpart:
            lines.append("  Paired position (action may be needed):")
            lines.append(
                f"    {counterpart.exchange_name}  {counterpart.ticker}"
                f"  {counterpart.direction}  {counterpart.amount}"
            )
    return "\n".join(lines)


def format_auto_close_success(
        trigger_exchange: str,
        trigger_ticker: str,
        close_exchange: str,
        close_ticker: str,
        close_amount: Decimal,
) -> str:
    return (
        f"<b>AUTO CLOSE OK</b>\n"
        f"Trigger: <b>{trigger_exchange}</b>  <code>{trigger_ticker}</code> reduced\n"
        f"Closed:  <b>{close_exchange}</b>  <code>{close_ticker}</code>  qty <code>{close_amount}</code>"
    )


def format_auto_close_failed(
        trigger_exchange: str,
        trigger_ticker: str,
        close_exchange: str,
        close_ticker: str,
        close_amount: Decimal,
) -> str:
    return (
        f"<b>AUTO CLOSE FAILED</b>\n"
        f"Trigger: <b>{trigger_exchange}</b>  <code>{trigger_ticker}</code> reduced\n"
        f"Failed to close: <b>{close_exchange}</b>  <code>{close_ticker}</code>  qty <code>{close_amount}</code>\n"
        f"Manual intervention required."
    )


def format_session_start_separator() -> str:
    return "<b>— — — NEW SESSION STARTED — — —</b>\nPrevious reduction alerts above are outdated."


def format_exchange_state(state: ExchangeState) -> str:
    lines: list[str] = []

    # Header
    lines.append(f"<b><code>{state.name}</code></b>")
    lines.append("")

    # Margin
    lines.append("<b>Margin</b>")
    if state.maintenance_margin > 0:
        lines.append(f"  Current:  <code>{_fmt_num(state.current_margin)}</code>")
        lines.append(f"  Required: <code>{_fmt_num(state.maintenance_margin)}</code>")
        ratio_emoji, ratio_text = _fmt_margin_ratio(state.margin_ratio)
        lines.append(f"  Ratio:    <code>{ratio_emoji} {ratio_text}</code>")
    else:
        lines.append(f"  Current:  <code>{_fmt_num(state.current_margin)}</code>")
        lines.append(f"  Required: <code>{_fmt_num(state.maintenance_margin)}</code>")
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
            emoji_a = "🟢" if pos.direction == "long" else "🔴"
            # 📈 когда позиция в плюсе (независимо от направления), 📉 когда в минусе
            trend_emoji = "\U0001f4c8" if pnl >= 0 else "\U0001f4c9"
            sign = "+" if pnl >= 0 else ""
            lines.append(
                f"  <code>{pos.ticker:<10}</code> {emoji_a:<5} "
                f"<code>{pos.amount}</code>  "
                f"{trend_emoji} {sign}{float(pnl):.2f}%"
            )
    lines.append("")

    # Updated — берём наибольшее из двух времён
    latest = max(
        state.positions_update_time,
        state.maintenance_margin_update_time,
    )
    latest_local = _to_notify_tz(latest)
    tz_label = latest_local.strftime("%Z") or latest_local.strftime("%z")
    lines.append(f"Updated: {latest_local.strftime('%H:%M:%S.%f')[:-3]} {tz_label}")

    return "\n".join(lines)


def format_leg_not_found(exchange: str, ticker: str) -> str:
    return (
        f"<b>STRUCTURE LEG NOT FOUND</b> — {exchange}\n"
        f"Ticker {ticker} not found in open positions.\n"
        f"Structure will treat this leg as amount=0."
    )


def format_structure_imbalance(
        trigger_exchange: str,
        trigger_ticker: str,
        old_amount: Decimal,
        new_amount: Decimal,
        closed_legs: list[dict],
        event_count: int = 1,
) -> str:
    lines = [f"<b>STRUCTURE IMBALANCED</b> ({event_count} event(s))"]
    lines.append("")
    lines.append(f"Trigger: <b>{trigger_exchange}</b>  <code>{trigger_ticker}</code>")
    amount_line = f"  Amount: <code>{old_amount}</code> → <code>{new_amount}</code>"
    if new_amount == Decimal(0):
        amount_line += "  <b>Fully closed</b>"
    lines.append(amount_line)
    if closed_legs:
        lines.append("")
        lines.append("Auto-closed:")
        for leg in closed_legs:
            lines.append(
                f"  <b>{leg['exchange']}</b>  <code>{leg['ticker']}</code>"
                f"  qty <code>{leg['amount']}</code>"
            )
    return "\n".join(lines)


def format_all_states_brief(states: dict[str, ExchangeState]) -> str:
    now_local = _to_notify_tz(datetime.now(tz=_tz.utc))
    tz_label = now_local.strftime("%Z") or now_local.strftime("%z")
    header = f"<b>Monitor</b>  |  {now_local.strftime('%H:%M:%S')} {tz_label}"

    if not states:
        return header

    # Собираем сырые значения для всех бирж
    rows = []
    for name, state in states.items():
        pos_count = len(state.positions)
        ratio_emoji, ratio_text = _fmt_margin_ratio(state.margin_ratio)
        margin_str = f"{_fmt_num(state.current_margin)} / {_fmt_num(state.maintenance_margin)}"
        pos_str = f"{pos_count} pos"
        rows.append((name.upper(), ratio_emoji, ratio_text, margin_str, pos_str))

    # Вычисляем максимальные ширины по каждой колонке.
    # Для ratio выравниваем только text-часть (числа), emoji выводим отдельно —
    # это гарантирует ровное выравнивание независимо от визуальной ширины emoji.
    max_name = max(len(r[0]) for r in rows)
    max_ratio_text = max(len(r[2]) for r in rows)
    max_margin = max(len(r[3]) for r in rows)
    max_pos = max(len(r[4]) for r in rows)

    pre_lines = []
    for name_upper, ratio_emoji, ratio_text, margin_str, pos_str in rows:
        name_col = name_upper.ljust(max_name)
        ratio_col = f"{ratio_emoji} {ratio_text.rjust(max_ratio_text)}"
        margin_col = margin_str.ljust(max_margin)
        pos_col = pos_str.ljust(max_pos)
        pre_lines.append(
            f"{name_col}  {ratio_col}  |  {margin_col}  |  {pos_col}"
        )
    pre_block = "<pre>" + "\n".join(pre_lines) + "</pre>"
    return header + "\n\n" + pre_block + "\n"


def format_structures_state(structures: list[Structure], states: dict[str, ExchangeState]) -> str:
    now_local = _to_notify_tz(datetime.now(tz=_tz.utc))
    tz_label = now_local.strftime("%Z") or now_local.strftime("%z")
    active = [s for s in structures if s.is_active]
    lines = [f"<b>STRUCTURES  |  {len(active)} active</b>"]
    if not active:
        lines.append("")
        lines.append("No active structures")
    else:
        lines.append("")
        for structure in active:
            leg_parts = []
            amounts = []
            tickers: set[str] = set()
            for leg in structure.legs:
                state = states.get(leg.exchange)
                pos = state.positions.get(leg.ticker) if state else None
                real_amount = pos.amount * leg.multiplier if pos else Decimal(0)
                emoji = "🟢" if (pos and pos.direction == "long") else "🔴"
                leg_parts.append(f"{emoji}{leg.exchange}")
                amounts.append(str(real_amount))
                tickers.add(leg.ticker)
            ticker_label = tickers.pop() if len(tickers) == 1 else "/".join(sorted(tickers))
            lines.append(
                f"  {ticker_label}  {' '.join(leg_parts)}  ({'/'.join(amounts)})"
            )
    lines.append("")
    lines.append(f"Updated: {now_local.strftime('%H:%M:%S')} {tz_label}")
    return "\n".join(lines)
