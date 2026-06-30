"""RiskManager subclass that swaps live DB/exchange I/O for in-memory state.

approve_trade()/calculate_position_size()/calculate_stops() are inherited
verbatim from RiskManager — the backtest enforces the identical gating
logic as production, not a re-implementation that could silently drift.
Only the 5 methods that do I/O in the live version are overridden here.
"""
from __future__ import annotations

from backtest.state import BacktestState
from execution.risk_manager import RiskConfig, RiskManager


class SimulatedRiskManager(RiskManager):
    def __init__(
        self,
        state: BacktestState,
        initial_balance: float,
        config: RiskConfig | None = None,
    ) -> None:
        self.state = state
        self.config = config or RiskConfig()
        self._initial_balance = initial_balance
        # No client/engine — never used; every overridden method below
        # reads/writes self.state instead.

    def get_account_balance(self) -> float | None:
        return self.state.balance

    def has_open_position(self, symbol: str) -> bool:
        return symbol in self.state.positions

    def get_open_position_count(self) -> int:
        return len(self.state.positions)

    def get_atr(self, symbol: str) -> float:
        return self.state.current_atr.get(symbol, 0.0)

    def check_drawdown(self) -> tuple[float, bool]:
        total_pnl = sum(t["pnl"] for t in self.state.closed_trades)
        drawdown = abs(total_pnl) / self._initial_balance if total_pnl < 0 else 0.0
        return drawdown, drawdown >= self.config.max_drawdown_pct
