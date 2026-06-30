"""Unit tests for backtest.metrics — pure functions over synthetic data.

Run:
    poetry run pytest tests/test_backtest_metrics.py -v
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from backtest.metrics import (
    max_drawdown,
    profit_factor,
    sharpe_ratio,
    total_return,
    win_rate,
)


def _equity_curve(values):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [(start + timedelta(hours=i), v) for i, v in enumerate(values)]


class TestWinRate:
    def test_mixed_trades(self):
        trades = [{"pnl": 10.0}, {"pnl": -5.0}, {"pnl": 20.0}]
        assert win_rate(trades) == pytest.approx(2 / 3)

    def test_no_trades_returns_zero(self):
        assert win_rate([]) == 0.0

    def test_all_wins(self):
        assert win_rate([{"pnl": 1.0}, {"pnl": 2.0}]) == pytest.approx(1.0)


class TestProfitFactor:
    def test_mixed_trades(self):
        trades = [{"pnl": 10.0}, {"pnl": -5.0}, {"pnl": 20.0}]
        assert profit_factor(trades) == pytest.approx(30.0 / 5.0)

    def test_no_losses_returns_inf(self):
        trades = [{"pnl": 10.0}, {"pnl": 5.0}]
        assert profit_factor(trades) == float("inf")

    def test_no_trades_returns_zero(self):
        assert profit_factor([]) == 0.0

    def test_no_profits_returns_zero(self):
        trades = [{"pnl": -10.0}, {"pnl": -5.0}]
        assert profit_factor(trades) == 0.0


class TestMaxDrawdown:
    def test_drawdown_from_peak(self):
        equity_curve = _equity_curve([1000.0, 1200.0, 900.0, 1100.0])
        assert max_drawdown(equity_curve) == pytest.approx(0.25)

    def test_monotonic_increase_has_zero_drawdown(self):
        equity_curve = _equity_curve([1000.0, 1100.0, 1200.0])
        assert max_drawdown(equity_curve) == pytest.approx(0.0)

    def test_empty_curve_returns_zero(self):
        assert max_drawdown([]) == 0.0


class TestTotalReturn:
    def test_positive_return(self):
        equity_curve = _equity_curve([1000.0, 1500.0])
        assert total_return(equity_curve) == pytest.approx(0.5)

    def test_negative_return(self):
        equity_curve = _equity_curve([1000.0, 800.0])
        assert total_return(equity_curve) == pytest.approx(-0.2)

    def test_empty_curve_returns_zero(self):
        assert total_return([]) == 0.0

    def test_single_point_returns_zero(self):
        assert total_return(_equity_curve([1000.0])) == 0.0


class TestSharpeRatio:
    def test_constant_equity_has_zero_sharpe(self):
        equity_curve = _equity_curve([1000.0, 1000.0, 1000.0])
        assert sharpe_ratio(equity_curve, periods_per_year=24 * 365) == pytest.approx(0.0)

    def test_rising_equity_has_positive_sharpe(self):
        equity_curve = _equity_curve([1000.0, 1010.0, 1025.0, 1015.0, 1040.0])
        assert sharpe_ratio(equity_curve, periods_per_year=24 * 365) > 0

    def test_falling_equity_has_negative_sharpe(self):
        equity_curve = _equity_curve([1000.0, 990.0, 975.0, 985.0, 960.0])
        assert sharpe_ratio(equity_curve, periods_per_year=24 * 365) < 0

    def test_fewer_than_two_points_returns_zero(self):
        assert sharpe_ratio(_equity_curve([1000.0]), periods_per_year=24 * 365) == 0.0
        assert sharpe_ratio([], periods_per_year=24 * 365) == 0.0
