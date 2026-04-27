"""
Tests for run.py: legacy path (raw -> BacktestRun -> Kiploks payload) and metrics from raw["results"].
Run from repo root: npm run freqtrade-integration:test
Or: cd kiploks-freqtrade && python3 test_run.py
Optional: python3 -m pytest kiploks-freqtrade/test_run.py -v (requires pytest installed)
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

# Allow importing run when executed as script or from repo root
_kiploks_ft = Path(__file__).resolve().parent
if _kiploks_ft not in sys.path:
    sys.path.insert(0, str(_kiploks_ft))

from run import (
    BacktestRun,
    build_kiploks_item,
    load_backtest_via_freqtrade_api,
    raw_to_backtest_runs,
    scan_backtest_results,
    validate_result_for_kiploks,
)
from run import (
    _freqtrade_trades_to_oos_trades,
    _parse_freqtrade_cmd,
    _path_under,
    _validate_strategy_name,
)


class TestLegacyPath(unittest.TestCase):
    """Legacy path: raw -> BacktestRun -> Kiploks payload."""

    def test_flat_raw_to_backtest_runs_and_payload_shape(self) -> None:
        """Flat raw -> raw_to_backtest_runs -> build_kiploks_item yields expected payload shape."""
        raw = {
            "backtest_start": "2024-01-01",
            "backtest_end": "2024-01-31",
            "config": {"timeframe": "5m", "pair_whitelist": ["BTC/USDT"]},
            "params": {"strategy": "TestStrategy"},
            "trades": [],
            "profit_total": 0.05,
            "total_trades": 10,
            "wins": 6,
            "sharpe_ratio": 1.2,
            "profit_factor": 1.5,
        }
        runs = raw_to_backtest_runs(raw, initial_balance=1000.0)
        self.assertEqual(len(runs), 1)
        run = runs[0]
        self.assertEqual(run.strategy_name, "TestStrategy")
        self.assertEqual(run.total_return, 0.05)
        self.assertEqual(run.total_trades, 10)
        self.assertEqual(run.win_rate, 0.6)
        self.assertEqual(run.sharpe_ratio, 1.2)
        self.assertEqual(run.profit_factor, 1.5)

        item = build_kiploks_item(run)
        self.assertIn("parameters", item)
        self.assertIn("backtestResult", item)
        self.assertIn("score", item)
        br = item["backtestResult"]
        self.assertIn("config", br)
        self.assertIn("results", br)
        self.assertIn("trades", br)
        self.assertIn("equityCurve", br)
        res = br["results"]
        self.assertEqual(res["totalReturn"], 0.05)
        self.assertEqual(res["winRate"], 0.6)
        self.assertEqual(res["sharpeRatio"], 1.2)
        self.assertEqual(res["profitFactor"], 1.5)
        self.assertEqual(res["totalTrades"], 10)

    def test_results_max_drawdown_reflected_in_payload(self) -> None:
        """raw['results']['max_drawdown'] is used by _get_metric and appears in payload maxDrawdown."""
        raw = {
            "backtest_start": "2024-01-01",
            "backtest_end": "2024-01-31",
            "config": {"timeframe": "5m", "pair_whitelist": ["BTC/USDT"]},
            "params": {"strategy": "TestStrategy"},
            "trades": [],
            "profit_total": 0.0,
            "results": {
                "max_drawdown": 0.15,
                "total_trades": 0,
            },
        }
        runs = raw_to_backtest_runs(raw, initial_balance=1000.0)
        self.assertEqual(len(runs), 1)
        run = runs[0]
        self.assertIsNotNone(run.max_drawdown)
        self.assertAlmostEqual(run.max_drawdown, 0.15)

        item = build_kiploks_item(run)
        self.assertEqual(item["backtestResult"]["results"]["maxDrawdown"], 0.15)


class TestApiAndScan(unittest.TestCase):
    """load_backtest_via_freqtrade_api and scan_backtest_results fallbacks."""

    def test_load_backtest_via_freqtrade_api_no_freqtrade_returns_empty(self) -> None:
        """Without Freqtrade env or with invalid path, load_backtest_via_freqtrade_api returns []."""
        path = Path(__file__).resolve()
        runs = load_backtest_via_freqtrade_api(path, initial_balance=1000.0)
        self.assertEqual(runs, [])

    def test_scan_backtest_results_nonexistent_returns_empty(self) -> None:
        """scan_backtest_results on dir without backtest_results returns []."""
        path = Path(__file__).resolve().parent / "nonexistent_dir_xyz"
        items = scan_backtest_results(path, top_n=3)
        self.assertEqual(items, [])


class TestParseFreqtradeCmd(unittest.TestCase):
    """_parse_freqtrade_cmd: allowlist, first token, injection prevention."""

    def test_valid_freqtrade_command(self) -> None:
        out = _parse_freqtrade_cmd("freqtrade backtest --config user_data/config.json")
        self.assertIsInstance(out, list)
        self.assertGreater(len(out), 0)
        self.assertTrue(out[0].lower().endswith("freqtrade") or "freqtrade" in out)

    def test_valid_docker_command(self) -> None:
        out = _parse_freqtrade_cmd("docker compose run --rm freqtrade")
        self.assertGreaterEqual(len(out), 3)
        self.assertEqual(out[0].lower(), "docker")

    def test_rejects_semicolon(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            _parse_freqtrade_cmd("freqtrade; rm -rf /")
        self.assertIn("invalid", str(ctx.exception).lower())

    def test_rejects_pipe(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            _parse_freqtrade_cmd("freqtrade | cat")
        self.assertIn("invalid", str(ctx.exception).lower())

    def test_rejects_disallowed_first_token(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            _parse_freqtrade_cmd("curl https://evil.com")
        self.assertIn("must start with", str(ctx.exception).lower())

    def test_empty_string_returns_empty_list(self) -> None:
        self.assertEqual(_parse_freqtrade_cmd(""), [])
        self.assertEqual(_parse_freqtrade_cmd("   "), [])


class TestPathUnder(unittest.TestCase):
    """_path_under: path traversal protection."""

    def test_path_under_base(self) -> None:
        base = Path("/tmp/backtest_results").resolve()
        p = base / "result.json"
        self.assertTrue(_path_under(p, base))

    def test_path_outside_base_false(self) -> None:
        base = Path("/tmp/backtest_results").resolve()
        p = Path("/etc/passwd")
        self.assertFalse(_path_under(p, base))

    def test_path_with_dot_dot_resolves_under_then_true(self) -> None:
        base = Path(__file__).resolve().parent
        p = base / "run.py"
        self.assertTrue(_path_under(p, base))

    def test_nonexistent_path_under_base_still_resolves_under(self) -> None:
        """Nonexistent file under base still resolves to a path under base (no OSError)."""
        base = Path(__file__).resolve().parent
        p = base / "nonexistent_xyz_123"
        self.assertTrue(_path_under(p, base))


class TestValidateStrategyName(unittest.TestCase):
    """_validate_strategy_name: allowed chars, length, subprocess safety."""

    def test_valid_name_returns_unchanged(self) -> None:
        self.assertEqual(_validate_strategy_name("SampleStrategy"), "SampleStrategy")
        self.assertEqual(_validate_strategy_name("A"), "A")
        self.assertEqual(_validate_strategy_name("Strategy_1"), "Strategy_1")

    def test_invalid_chars_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            _validate_strategy_name("Strategy-1")
        self.assertIn("Invalid strategy name", str(ctx.exception))
        with self.assertRaises(ValueError):
            _validate_strategy_name("strategy.with.dots")

    def test_empty_raises(self) -> None:
        with self.assertRaises(ValueError):
            _validate_strategy_name("")
        with self.assertRaises(ValueError):
            _validate_strategy_name("   ")

    def test_starts_with_digit_raises(self) -> None:
        with self.assertRaises(ValueError):
            _validate_strategy_name("1Strategy")

    def test_too_long_raises(self) -> None:
        with self.assertRaises(ValueError):
            _validate_strategy_name("A" + "b" * 128)


class TestFreqtradeTradesToOosTrades(unittest.TestCase):
    """Conversion of Freqtrade OOS trades to Kiploks oos_trades (net_return)."""

    def test_profit_ratio_mapped_to_net_return(self) -> None:
        trades = [
            {"close_date": "2024-01-02T10:00:00", "profit_ratio": 0.01, "profit_abs": 10},
            {"close_date": "2024-01-03T10:00:00", "profit_ratio": -0.005, "profit_abs": -5},
        ]
        out, balance = _freqtrade_trades_to_oos_trades(trades, initial_balance=1000.0)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["net_return"], 0.01)
        self.assertEqual(out[1]["net_return"], -0.005)
        self.assertEqual(balance, 1005.0)

    def test_empty_trades_returns_empty(self) -> None:
        out, balance = _freqtrade_trades_to_oos_trades([], initial_balance=1000.0)
        self.assertEqual(out, [])
        self.assertEqual(balance, 1000.0)


class TestValidateResultForKiploks(unittest.TestCase):
    """validate_result_for_kiploks: contract before upload."""

    def _minimal_valid_item(self) -> dict:
        return {
            "parameters": {"strategy": "SampleStrategy"},
            "backtestResult": {
                "config": {"symbol": "BTC/USDT"},
                "results": {"totalReturn": 0.0, "totalTrades": 0},
                "trades": [],
            },
            "walkForwardAnalysis": {
                "periods": [
                    {"optimizationReturn": 0.05, "validationReturn": 0.02},
                ],
            },
            "oos_trades": [{"net_return": 0.01}],
        }

    def test_valid_item_passes(self) -> None:
        ok, err = validate_result_for_kiploks(self._minimal_valid_item())
        self.assertTrue(ok, err)
        self.assertEqual(err, "")

    def test_empty_item_fails(self) -> None:
        ok, err = validate_result_for_kiploks(None)
        self.assertFalse(ok)
        self.assertIn("Empty", err)
        ok, err = validate_result_for_kiploks({})
        self.assertFalse(ok)

    def test_missing_backtest_result_fails(self) -> None:
        item = self._minimal_valid_item()
        del item["backtestResult"]
        ok, err = validate_result_for_kiploks(item)
        self.assertFalse(ok)
        self.assertIn("backtestResult", err)

    def test_missing_results_fails(self) -> None:
        item = self._minimal_valid_item()
        item["backtestResult"]["results"] = None
        ok, err = validate_result_for_kiploks(item)
        self.assertFalse(ok)
        self.assertIn("results", err)

    def test_missing_symbol_fails(self) -> None:
        item = self._minimal_valid_item()
        item["backtestResult"]["config"] = {}
        ok, err = validate_result_for_kiploks(item)
        self.assertFalse(ok)
        self.assertIn("symbol", err)

    def test_missing_parameters_strategy_fails(self) -> None:
        item = self._minimal_valid_item()
        item["parameters"] = {}
        ok, err = validate_result_for_kiploks(item)
        self.assertFalse(ok)
        self.assertIn("strategy", err)

    def test_missing_walk_forward_analysis_fails(self) -> None:
        item = self._minimal_valid_item()
        del item["walkForwardAnalysis"]
        ok, err = validate_result_for_kiploks(item)
        self.assertFalse(ok)
        self.assertIn("walkForwardAnalysis", err)

    def test_missing_or_empty_oos_trades_fails(self) -> None:
        item = self._minimal_valid_item()
        del item["oos_trades"]
        ok, err = validate_result_for_kiploks(item)
        self.assertFalse(ok)
        self.assertIn("oos_trades", err)
        item["oos_trades"] = []
        ok, err = validate_result_for_kiploks(item)
        self.assertFalse(ok)
        self.assertIn("oos_trades", err)

    def test_empty_periods_fails(self) -> None:
        item = self._minimal_valid_item()
        item["walkForwardAnalysis"]["periods"] = []
        ok, err = validate_result_for_kiploks(item)
        self.assertFalse(ok)
        self.assertIn("periods", err)

    def test_period_missing_optimization_return_fails(self) -> None:
        item = self._minimal_valid_item()
        item["walkForwardAnalysis"]["periods"][0]["optimizationReturn"] = None
        ok, err = validate_result_for_kiploks(item)
        self.assertFalse(ok)
        self.assertIn("optimizationReturn", err)


if __name__ == "__main__":
    unittest.main(verbosity=2)
