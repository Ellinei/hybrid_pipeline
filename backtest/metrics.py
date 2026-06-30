"""Pure performance metrics over a trade log + equity curve."""
from __future__ import annotations

import math
from datetime import datetime


def win_rate(trades: list[dict]) -> float:
    if not trades:
        return 0.0
    wins = sum(1 for t in trades if t["pnl"] > 0)
    return wins / len(trades)


def profit_factor(trades: list[dict]) -> float:
    gross_profit = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gross_loss = -sum(t["pnl"] for t in trades if t["pnl"] < 0)
    if gross_loss == 0:
        return float("inf") if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def max_drawdown(equity_curve: list[tuple[datetime, float]]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0][1]
    worst = 0.0
    for _, equity in equity_curve:
        peak = max(peak, equity)
        if peak > 0:
            worst = max(worst, (peak - equity) / peak)
    return worst


def total_return(equity_curve: list[tuple[datetime, float]]) -> float:
    if len(equity_curve) < 2:
        return 0.0
    start = equity_curve[0][1]
    end = equity_curve[-1][1]
    if start == 0:
        return 0.0
    return (end - start) / start


def sharpe_ratio(
    equity_curve: list[tuple[datetime, float]], periods_per_year: float
) -> float:
    """Annualized Sharpe ratio (zero risk-free rate) over per-period returns."""
    if len(equity_curve) < 2:
        return 0.0

    returns = []
    for (_, prev), (_, curr) in zip(equity_curve, equity_curve[1:]):
        if prev != 0:
            returns.append((curr - prev) / prev)

    if len(returns) < 2:
        return 0.0

    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    std = math.sqrt(variance)
    if std == 0:
        return 0.0

    return (mean / std) * math.sqrt(periods_per_year)
