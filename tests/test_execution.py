"""Unit tests for the execution package.

No real exchange calls or DB connections — everything is mocked.

Run:
    poetry run pytest tests/test_execution.py -v
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from execution.risk_manager import RiskConfig, RiskManager
from execution.trade_executor import TradeExecutor
from signals.base import AggregatedSignal, Signal, SignalDirection


def _make_signal(direction=SignalDirection.BUY, confidence=0.9, symbol="BTCUSDT"):
    return AggregatedSignal(
        direction=direction,
        confidence=confidence,
        symbol=symbol,
        timestamp=datetime.now(timezone.utc),
        signals=[
            Signal(direction, confidence, "technical", symbol, datetime.now(timezone.utc)),
            Signal(direction, confidence, "ml", symbol, datetime.now(timezone.utc)),
            Signal(direction, confidence, "sentiment", symbol, datetime.now(timezone.utc)),
        ],
        reasoning="test",
        weights_used={},
    )


def _make_engine_returning(scalar_value):
    """A fake SQLAlchemy engine whose connect().execute(...).scalar() returns scalar_value."""
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = scalar_value
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    return engine


def _symbol_info(step_size="0.0001", min_qty="0.0001", tick_size="0.01", min_notional="10"):
    return {
        "symbol": "BTCUSDT",
        "filters": [
            {"filterType": "LOT_SIZE", "stepSize": step_size, "minQty": min_qty, "maxQty": "9000"},
            {"filterType": "PRICE_FILTER", "tickSize": tick_size, "minPrice": "0.01", "maxPrice": "1000000"},
            {"filterType": "MIN_NOTIONAL", "minNotional": min_notional},
        ],
    }


class _FakeConn:
    """Routes execute() calls by SQL content so a single fake connection can
    back the whole lifecycle (table bootstrap, open-trade lookup, insert,
    update) within one execute_signal() call."""

    def __init__(self, open_trade_row=None, scalar_value=0):
        self.open_trade_row = open_trade_row
        self.scalar_value = scalar_value
        self.calls = []  # list of (sql, params)

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.calls.append((sql, params))
        result = MagicMock()
        if "SELECT id, quantity" in sql:
            result.fetchone.return_value = (
                MagicMock(_mapping=self.open_trade_row) if self.open_trade_row else None
            )
        elif "INSERT INTO raw.trades" in sql:
            result.fetchone.return_value = MagicMock(
                _mapping={"id": 1, "executed_at": datetime.now(timezone.utc)}
            )
        else:
            result.scalar.return_value = self.scalar_value
        return result

    def commit(self):
        pass


def _make_fake_engine(open_trade_row=None, scalar_value=0):
    conn = _FakeConn(open_trade_row=open_trade_row, scalar_value=scalar_value)
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    return engine, conn


def _make_approved_risk(client, engine, atr=5.0, has_open_position=True):
    risk = RiskManager(client=client, db_engine=engine, config=RiskConfig(min_confidence=0.0))
    risk.get_atr = MagicMock(return_value=atr)
    risk.get_open_position_count = MagicMock(return_value=0)
    risk.check_drawdown = MagicMock(return_value=(0.0, False))
    risk.has_open_position = MagicMock(return_value=has_open_position)
    return risk


# ===========================================================================
# 1. RiskManager.calculate_position_size — notional cannot exceed balance
# ===========================================================================

class TestCalculatePositionSize:
    def test_caps_size_to_available_balance_when_atr_is_tiny(self):
        risk = RiskManager(client=MagicMock(), db_engine=MagicMock())
        size = risk.calculate_position_size(price=100.0, atr=0.0001, balance=1_000.0)
        assert size * 100.0 <= 1_000.0 + 1e-6

    def test_returns_zero_when_atr_is_zero(self):
        risk = RiskManager(client=MagicMock(), db_engine=MagicMock())
        assert risk.calculate_position_size(price=100.0, atr=0.0, balance=1_000.0) == 0.0

    def test_returns_zero_when_notional_below_minimum(self):
        risk = RiskManager(client=MagicMock(), db_engine=MagicMock())
        size = risk.calculate_position_size(price=100.0, atr=1000.0, balance=1_000.0)
        assert size == 0.0


# ===========================================================================
# 1b. RiskManager.calculate_stops — shared pure formula (live + backtest)
# ===========================================================================

class TestCalculateStops:
    def test_buy_stop_below_and_target_above_price(self):
        risk = RiskManager(client=MagicMock(), db_engine=MagicMock())
        stop_loss, take_profit = risk.calculate_stops(price=100.0, atr=5.0)
        # defaults: stop_loss_atr_mult=1.5, take_profit_atr_mult=3.0
        assert stop_loss == pytest.approx(100.0 - 5.0 * 1.5)
        assert take_profit == pytest.approx(100.0 + 5.0 * 3.0)

    def test_uses_configured_multiples(self):
        risk = RiskManager(
            client=MagicMock(), db_engine=MagicMock(),
            config=RiskConfig(stop_loss_atr_mult=2.0, take_profit_atr_mult=4.0),
        )
        stop_loss, take_profit = risk.calculate_stops(price=100.0, atr=10.0)
        assert stop_loss == pytest.approx(80.0)
        assert take_profit == pytest.approx(140.0)


# ===========================================================================
# 2. RiskManager.approve_trade — fail-closed gates
# ===========================================================================

class TestApproveTrade:
    def test_rejects_when_balance_unavailable(self):
        client = MagicMock()
        client.get_account_balance.side_effect = Exception("api down")
        engine = _make_engine_returning(0)
        risk = RiskManager(client=client, db_engine=engine)

        approved, reason = risk.approve_trade(_make_signal(), price=100.0)

        assert approved is False
        assert "balance" in reason

    def test_rejects_sell_with_no_open_position(self):
        client = MagicMock()
        client.get_account_balance.return_value = 1_000.0
        engine = _make_engine_returning(0)  # COUNT(*) == 0 -> no open position
        risk = RiskManager(client=client, db_engine=engine)

        approved, reason = risk.approve_trade(
            _make_signal(direction=SignalDirection.SELL), price=100.0
        )

        assert approved is False
        assert "no open position" in reason

    def test_rejects_hold_signal(self):
        risk = RiskManager(client=MagicMock(), db_engine=MagicMock())
        approved, reason = risk.approve_trade(
            _make_signal(direction=SignalDirection.HOLD), price=100.0
        )
        assert approved is False
        assert "HOLD" in reason

    def test_rejects_low_confidence(self):
        risk = RiskManager(client=MagicMock(), db_engine=MagicMock())
        approved, reason = risk.approve_trade(
            _make_signal(confidence=0.1), price=100.0
        )
        assert approved is False
        assert "confidence" in reason


# ===========================================================================
# 3. TradeExecutor — live_orders_enabled defaults
# ===========================================================================

class TestLiveOrdersEnabledDefault:
    def test_disabled_by_default(self, monkeypatch):
        monkeypatch.delenv("LIVE_ORDERS_ENABLED", raising=False)
        engine, _ = _make_fake_engine()
        executor = TradeExecutor(client=MagicMock(), risk_manager=MagicMock(), db_engine=engine)
        assert executor.live_orders_enabled is False

    def test_enabled_when_env_set_true(self, monkeypatch):
        monkeypatch.setenv("LIVE_ORDERS_ENABLED", "true")
        engine, _ = _make_fake_engine()
        executor = TradeExecutor(client=MagicMock(), risk_manager=MagicMock(), db_engine=engine)
        assert executor.live_orders_enabled is True

    def test_explicit_constructor_arg_overrides_env(self, monkeypatch):
        monkeypatch.setenv("LIVE_ORDERS_ENABLED", "true")
        engine, _ = _make_fake_engine()
        executor = TradeExecutor(
            client=MagicMock(), risk_manager=MagicMock(), db_engine=engine,
            live_orders_enabled=False,
        )
        assert executor.live_orders_enabled is False


# ===========================================================================
# 4. TradeExecutor._execute_buy_entry — OCO placement after fill
# ===========================================================================

class TestBuyEntryWithOCO:
    def _client(self, price=100.0, balance=1_000.0):
        client = MagicMock()
        client.get_current_price.return_value = price
        client.get_account_balance.return_value = balance
        client.get_symbol_info_cached.return_value = _symbol_info()
        client.client.order_market_buy.return_value = {
            "symbol": "BTCUSDT",
            "executedQty": "2.6666",
            "fills": [
                {"price": "100.0", "qty": "2.6666", "commission": "0.0026666", "commissionAsset": "BTC"}
            ],
        }
        client.client.create_oco_order.return_value = {"orderListId": 12345}
        return client

    def test_successful_oco_placement_records_order_list_id(self, monkeypatch):
        monkeypatch.setenv("LIVE_ORDERS_ENABLED", "true")
        client = self._client()
        engine, conn = _make_fake_engine()
        risk = _make_approved_risk(client, engine)
        executor = TradeExecutor(client=client, risk_manager=risk, db_engine=engine)

        result = executor.execute_signal(_make_signal(), dry_run=False)

        assert result is not None
        assert result["status"] == "open"
        assert result["oco_order_list_id"] == 12345
        insert_params = next(p for sql, p in conn.calls if "INSERT INTO raw.trades" in sql)
        assert insert_params["oco_order_list_id"] == 12345

    def test_oco_failure_after_fill_logs_critical_and_records_unprotected_open_trade(self, monkeypatch):
        monkeypatch.setenv("LIVE_ORDERS_ENABLED", "true")
        client = self._client()
        client.client.create_oco_order.side_effect = Exception("exchange rejected OCO")
        engine, conn = _make_fake_engine()
        risk = _make_approved_risk(client, engine)
        executor = TradeExecutor(client=client, risk_manager=risk, db_engine=engine)

        result = executor.execute_signal(_make_signal(), dry_run=False)

        # The dangerous case must stay visible, not silently dropped.
        assert result is not None
        assert result["status"] == "open"
        assert result["oco_order_list_id"] is None

    def test_qty_rounded_down_to_lot_size_before_buy_order(self, monkeypatch):
        monkeypatch.setenv("LIVE_ORDERS_ENABLED", "true")
        client = self._client()
        client.get_symbol_info_cached.return_value = _symbol_info(step_size="0.001")
        engine, conn = _make_fake_engine()
        risk = _make_approved_risk(client, engine)
        executor = TradeExecutor(client=client, risk_manager=risk, db_engine=engine)

        executor.execute_signal(_make_signal(), dry_run=False)

        _, kwargs = client.client.order_market_buy.call_args
        # step 0.001 -> at most 3 decimal places, never the raw 6-decimal calc.
        assert round(kwargs["quantity"], 3) == kwargs["quantity"]

    def test_rejects_trade_when_notional_below_minimum_after_rounding(self, monkeypatch):
        monkeypatch.setenv("LIVE_ORDERS_ENABLED", "true")
        client = self._client(balance=5.0)  # tiny balance -> tiny qty -> below MIN_NOTIONAL
        engine, conn = _make_fake_engine()
        risk = _make_approved_risk(client, engine)
        executor = TradeExecutor(client=client, risk_manager=risk, db_engine=engine)

        result = executor.execute_signal(_make_signal(), dry_run=False)

        assert result is None
        client.client.order_market_buy.assert_not_called()

    def test_dry_run_records_without_touching_exchange(self):
        client = self._client()
        engine, conn = _make_fake_engine()
        risk = _make_approved_risk(client, engine)
        executor = TradeExecutor(client=client, risk_manager=risk, db_engine=engine)

        result = executor.execute_signal(_make_signal(), dry_run=True)

        assert result is not None
        assert result["status"] == "dry_run"
        client.client.order_market_buy.assert_not_called()

    def test_live_orders_disabled_blocks_order_placement(self):
        client = self._client()
        engine, conn = _make_fake_engine()
        risk = _make_approved_risk(client, engine)
        executor = TradeExecutor(
            client=client, risk_manager=risk, db_engine=engine, live_orders_enabled=False
        )

        result = executor.execute_signal(_make_signal(), dry_run=False)

        assert result is None
        client.client.order_market_buy.assert_not_called()


# ===========================================================================
# 5. TradeExecutor._execute_sell_close — close the ACTUAL held quantity
# ===========================================================================

class TestSellCloseUsesActualHeldQuantity:
    def _open_trade_row(self, order_list_id=999):
        return {"id": 5, "quantity": 0.05, "entry_price": 100.0, "oco_order_list_id": order_list_id}

    def test_close_sells_recorded_quantity_not_recomputed_size(self, monkeypatch):
        monkeypatch.setenv("LIVE_ORDERS_ENABLED", "true")
        client = MagicMock()
        client.get_current_price.return_value = 110.0
        client.client.order_market_sell.return_value = {
            "symbol": "BTCUSDT", "executedQty": "0.05",
            "fills": [{"price": "110.0", "qty": "0.05", "commission": "0.0", "commissionAsset": "USDT"}],
        }
        engine, conn = _make_fake_engine(open_trade_row=self._open_trade_row())
        risk = _make_approved_risk(client, engine, has_open_position=True)
        risk.calculate_position_size = MagicMock(return_value=0.5)  # must NOT be used
        executor = TradeExecutor(client=client, risk_manager=risk, db_engine=engine)

        executor.execute_signal(_make_signal(direction=SignalDirection.SELL), dry_run=False)

        risk.calculate_position_size.assert_not_called()
        _, kwargs = client.client.order_market_sell.call_args
        assert kwargs["quantity"] == 0.05

    def test_cancels_oco_before_market_sell_when_order_list_id_present(self, monkeypatch):
        monkeypatch.setenv("LIVE_ORDERS_ENABLED", "true")
        client = MagicMock()
        client.get_current_price.return_value = 110.0
        client.client.order_market_sell.return_value = {
            "symbol": "BTCUSDT", "executedQty": "0.05", "fills": [],
        }
        engine, conn = _make_fake_engine(open_trade_row=self._open_trade_row(order_list_id=999))
        risk = _make_approved_risk(client, engine, has_open_position=True)
        executor = TradeExecutor(client=client, risk_manager=risk, db_engine=engine)

        executor.execute_signal(_make_signal(direction=SignalDirection.SELL), dry_run=False)

        names = [c[0] for c in client.client.mock_calls]
        assert names.index("v3_delete_order_list") < names.index("order_market_sell")
        _, kwargs = client.client.v3_delete_order_list.call_args
        assert kwargs["orderListId"] == 999

    def test_oco_cancel_failure_treated_as_already_closed_externally(self, monkeypatch):
        monkeypatch.setenv("LIVE_ORDERS_ENABLED", "true")
        client = MagicMock()
        client.get_current_price.return_value = 110.0
        client.client.v3_delete_order_list.side_effect = Exception("Unknown order list")
        engine, conn = _make_fake_engine(open_trade_row=self._open_trade_row(order_list_id=999))
        risk = _make_approved_risk(client, engine, has_open_position=True)
        executor = TradeExecutor(client=client, risk_manager=risk, db_engine=engine)

        result = executor.execute_signal(_make_signal(direction=SignalDirection.SELL), dry_run=False)

        client.client.order_market_sell.assert_not_called()
        assert result["status"] == "closed_externally"

    def test_close_updates_existing_row_instead_of_inserting_new_one(self, monkeypatch):
        monkeypatch.setenv("LIVE_ORDERS_ENABLED", "true")
        client = MagicMock()
        client.get_current_price.return_value = 110.0
        client.client.order_market_sell.return_value = {
            "symbol": "BTCUSDT", "executedQty": "0.05", "fills": [],
        }
        engine, conn = _make_fake_engine(open_trade_row=self._open_trade_row(order_list_id=None))
        risk = _make_approved_risk(client, engine, has_open_position=True)
        executor = TradeExecutor(client=client, risk_manager=risk, db_engine=engine)

        executor.execute_signal(_make_signal(direction=SignalDirection.SELL), dry_run=False)

        sqls = [sql for sql, _ in conn.calls]
        assert any("UPDATE raw.trades" in sql for sql in sqls)
        assert not any("INSERT INTO raw.trades" in sql for sql in sqls)

    def test_pnl_computed_correctly_on_close(self, monkeypatch):
        monkeypatch.setenv("LIVE_ORDERS_ENABLED", "true")
        client = MagicMock()
        client.get_current_price.return_value = 110.0
        client.client.order_market_sell.return_value = {
            "symbol": "BTCUSDT", "executedQty": "2", "fills": [],
        }
        row = {"id": 5, "quantity": 2.0, "entry_price": 100.0, "oco_order_list_id": None}
        engine, conn = _make_fake_engine(open_trade_row=row)
        risk = _make_approved_risk(client, engine, has_open_position=True)
        executor = TradeExecutor(client=client, risk_manager=risk, db_engine=engine)

        result = executor.execute_signal(_make_signal(direction=SignalDirection.SELL), dry_run=False)

        assert result["pnl"] == pytest.approx(20.0)
