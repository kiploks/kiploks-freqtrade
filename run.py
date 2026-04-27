#!/usr/bin/env python3
"""
Kiploks.com integration for Freqtrade.

Scans user_data/backtest_results/ (and optionally wfa_results/), picks top_n results,
optionally generates WFA by running backtests per window, normalizes to Kiploks format,
and uploads to Kiploks via integration API (POST /api/integration/results) when api_token and api_url are set.
No research_id required; you get short analyze links in the response.

Usage: python run.py  (set FREQTRADE_USER_DATA if needed)
For WFA: run with the same Python/env you use for freqtrade (e.g. activate venv then python run.py), or set FREQTRADE_CMD.
Export: full payload is always written to kiploks-freqtrade/export_test_result.json (same folder as uploaded.json).

Cross-platform: Path() for paths; Docker mount uses _path_for_docker_mount (Windows -> /mnt/c/...); subprocess always shell=False; all file opens use encoding="utf-8". No fcntl/msvcrt (TOCTOU handled by retry).
"""

from __future__ import annotations

__version__ = "1.0.0"
__all__ = [
    "scan_backtest_results",
    "generate_wfa_for_top",
    "upload_to_kiploks",
    "export_test_result_to_json",
    "load_config",
]

import io
import json
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

try:
    from pydantic import BaseModel
    from pydantic import ValidationError as PydanticValidationError
    from pydantic import field_validator, model_validator
    _PYDANTIC_AVAILABLE = True
except ImportError:
    BaseModel = None  # type: ignore[misc, assignment]
    PydanticValidationError = None  # type: ignore[misc, assignment]
    field_validator = None  # type: ignore[assignment]
    model_validator = None  # type: ignore[assignment]
    _PYDANTIC_AVAILABLE = False

# Prefix for backtest result files created by this script (WFA). Only these are pruned by keep_last_n_backtest_files.
KIPLOKS_BACKTEST_PREFIX = "kiploks-backtest-result"

# Paths created/renamed by this run (WFA). Deleted on KeyboardInterrupt so no leftovers.
# Single-run use only; do not import this module in a long-lived process that runs multiple analyses.
_created_by_this_run: list[Path] = []

# Match YYYY-MM-DD in backtest result filename (e.g. backtest-result-2026-02-09_08-26-01.json or backtest_results_2020-09-27_16-20-48.json)
_BACKTEST_FILENAME_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")

# Max backtest file size (plain or ZIP uncompressed total) and max files in ZIP (DoS / zip-bomb protection).
_MAX_BACKTEST_FILE_SIZE = 50 * 1024 * 1024  # 50 MB
_MAX_ZIP_FILES = 100

# Retry backoff for TOCTOU file reads (race with Freqtrade / Docker). No fcntl.
_FILE_READ_RETRY_BASE_SLEEP = 0.1
_MAX_TOTAL_WAIT = 2.0  # Cap total retry time to avoid long hangs


def _path_under(p: Path, base: Path) -> bool:
    """True if p resolves to a path under base (path traversal protection). Returns False on broken symlinks."""
    try:
        return p.resolve().is_relative_to(base)
    except (ValueError, OSError):
        return False


def _read_path_text_with_retry(path: Path, encoding: str = "utf-8") -> str | None:
    """Read file content with retry on FileNotFoundError (TOCTOU race). Returns None if missing after retries or on OSError.
    Single-threaded use only; not thread-safe."""
    attempt = 0
    elapsed = 0.0
    while elapsed < _MAX_TOTAL_WAIT:
        try:
            with open(path, "r", encoding=encoding) as f:
                return f.read()
        except FileNotFoundError:
            if elapsed + _FILE_READ_RETRY_BASE_SLEEP >= _MAX_TOTAL_WAIT:
                return None
            sleep_time = min(_FILE_READ_RETRY_BASE_SLEEP * (2 ** attempt), 0.5)
            sleep_time = min(sleep_time, _MAX_TOTAL_WAIT - elapsed)
            time.sleep(sleep_time)
            elapsed += sleep_time
            attempt += 1
        except OSError as e:
            _log(f"Cannot read {path}: {e}")
            return None
    return None


def _date_from_backtest_filename(path: Path) -> str:
    """Extract YYYY-MM-DD from backtest result filename. Returns '0000-01-01' if no date found."""
    match = _BACKTEST_FILENAME_DATE_RE.search(path.stem)
    if match:
        return match.group(1)
    return "0000-01-01"


def get_repo_root() -> Path:
    """Repo root (parent of kiploks-freqtrade). Script lives in repo_root/kiploks-freqtrade/run.py."""
    return Path(__file__).resolve().parent.parent


def get_user_data_root() -> Path:
    """user_data is always repo_root/user_data. Error if missing (checked in main)."""
    return get_repo_root() / "user_data"


def load_config() -> dict:
    """Load config from the same folder as run.py. Expects kiploks.json (copy from kiploks.json.example)."""
    base = Path(__file__).resolve().parent
    json_path = base / "kiploks.json"
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as e:
        print(f"Invalid kiploks.json: {e}", file=sys.stderr)
        return {}


def _read_freqtrade_config(user_data: Path) -> dict:
    """Read user_data/config.json. Returns {} if missing or invalid. No is_file() before open (TOCTOU-safe)."""
    config_path = user_data / "config.json"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (FileNotFoundError, OSError):
        return {}
    except Exception:
        return {}


def get_initial_balance_from_freqtrade_config(user_data: Path) -> float:
    """
    Read initial balance (dry_run_wallet) from Freqtrade user_data/config.json.
    This is the balance used when running backtest/strategy. Default 1000.0.
    """
    data = _read_freqtrade_config(user_data)
    wallet = data.get("dry_run_wallet")
    if isinstance(wallet, (int, float)) and wallet > 0:
        return float(wallet)
    return 1000.0


def get_exchange_from_freqtrade_config(user_data: Path) -> str | None:
    """Read exchange name from Freqtrade user_data/config.json. Returns None if missing.
    Supports exchange as object with .name or string.
    """
    data = _read_freqtrade_config(user_data)
    if not data:
        return None
    ex = data.get("exchange")
    if isinstance(ex, dict) and ex:
        name = ex.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    if isinstance(ex, str) and ex.strip():
        return ex.strip()
    name = data.get("exchange_name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return None


def _read_config_from_backtest_path(path: Path) -> dict | None:
    """Read config JSON from backtest result (ZIP: *_config.json inside archive; JSON: sibling _config.json). Returns None if missing."""
    try:
        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path, "r") as zf:
                config_names = [
                    n for n in zf.namelist()
                    if n.endswith(".json") and "_config" in n and ".." not in n and not n.startswith("/") and "\\" not in n
                ]
                if not config_names:
                    return None
                with io.TextIOWrapper(zf.open(config_names[0]), encoding="utf-8") as f:
                    data = json.load(f)
                return data if isinstance(data, dict) else None
        stem = path.stem
        config_path = path.parent / f"{stem}_config.json"
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else None
        except FileNotFoundError:
            return None
    except Exception:
        return None


def _fee_slippage_from_config_and_trades(
    config: dict | None, trades: list | None
) -> dict:
    """Extract commission/fee/slippage for Kiploks backend. Backend expects decimal (e.g. 0.001 = 0.1%).
    Sources: config (fee_open, fee_close, fee, trading_fee, commission, slippage), or first trade (fee_open, fee_close).
    """
    out: dict = {}
    c = config or {}
    for key in ("fee_open", "fee_close", "fee", "trading_fee", "commission", "slippage"):
        v = c.get(key)
        if v is None:
            continue
        if isinstance(v, (int, float)) and (v == v and abs(v) != float("inf")):
            out[key] = float(v)
        elif isinstance(v, str):
            s = v.replace("%", "").strip()
            try:
                val = float(s)
                if "%" in v:
                    val = val / 100.0
                out[key] = val
            except ValueError:
                pass
    if not out and trades and len(trades) > 0:
        t = trades[0]
        for key in ("fee_open", "fee_close"):
            v = t.get(key)
            if v is not None and isinstance(v, (int, float)):
                out[key] = float(v)
    return out


def _exchange_name_from_config(data: dict | None) -> str | None:
    """Extract exchange name from Freqtrade config (exchange.name or exchange string)."""
    if not data:
        return None
    ex = data.get("exchange")
    if isinstance(ex, dict) and ex:
        name = ex.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    if isinstance(ex, str) and ex.strip():
        return ex.strip()
    name = data.get("exchange_name")
    return name.strip() if isinstance(name, str) and name.strip() else None


def get_initial_balance_from_backtest_result_path(path: Path) -> float | None:
    """
    Read dry_run_wallet from the config embedded in a backtest result (ZIP or sibling _config.json).
    ZIP: looks for *_config.json inside the archive (e.g. backtest-result-2026-02-09_08-26-01_config.json).
    JSON: looks for {stem}_config.json in the same directory.
    Returns None if not found or invalid; caller should fall back to user_data config.
    """
    data = _read_config_from_backtest_path(path)
    if not data:
        return None
    wallet = data.get("dry_run_wallet")
    if isinstance(wallet, (int, float)) and wallet > 0:
        return float(wallet)
    return None


def get_exchange_from_backtest_result_path(path: Path) -> str | None:
    """Read exchange name from config embedded in backtest result (ZIP or sibling _config.json). Supports exchange.name."""
    return _exchange_name_from_config(_read_config_from_backtest_path(path))


# Default pair when symbol is missing or unparseable (single source for fallbacks).
_DEFAULT_PAIR = "BTC/USDT"


def _symbol_to_pair(symbol: str, ref_market: str | None = None) -> str:
    """Raw symbol to pair with slash (e.g. BTCUSDT -> BTC/USDT). Same logic as kiploks-octobot for parity."""
    s = (symbol or "").strip().upper()
    if not s:
        return _DEFAULT_PAIR
    if ref_market:
        q = ref_market.strip().upper()
        if q and len(s) > len(q) and s.endswith(q):
            return f"{s[:-len(q)]}/{q}"
    for suffix in (
        "USDT", "USDC", "BUSD", "FDUSD", "DAI",
        "BTC", "ETH", "BNB", "EUR", "USD", "GBP",
    ):
        if len(s) > len(suffix) and s.endswith(suffix):
            return f"{s[:-len(suffix)]}/{suffix}"
    return s


def _symbol_to_pair_file(symbol: str) -> str:
    """BTCUSDT -> BTC_USDT for Freqtrade data filename. Uses _symbol_to_pair for quote detection."""
    pair = _symbol_to_pair(symbol)
    return pair.replace("/", "_")


def _candle_timestamp(c: dict) -> int | float | None:
    """Extract timestamp from candle dict (t, timestamp, or time key)."""
    return c.get("t") or c.get("timestamp") or c.get("time")


def _trade_date_sort_key(t: dict) -> str:
    """Sort key for trades by close_date or open_date (for stable ordering)."""
    return t.get("close_date") or t.get("open_date") or ""


def _timeframe_to_ms(timeframe: str) -> int:
    """1m -> 60000, 5m -> 300000, 1h -> 3600000, 1d -> 86400000. Returns 0 if timeframe missing or unknown."""
    tf = (timeframe or "").strip().lower()
    if not tf:
        return 0
    if tf == "1m":
        return 60 * 1000
    if tf == "5m":
        return 5 * 60 * 1000
    if tf == "15m":
        return 15 * 60 * 1000
    if tf == "1h":
        return 60 * 60 * 1000
    if tf == "4h":
        return 4 * 60 * 60 * 1000
    if tf == "1d":
        return 24 * 60 * 60 * 1000
    return 0


def _raw_gap_density(candles: list[dict], timeframe_ms: int) -> dict:
    """DQ1 raw only - docs/DQG_RAW_PAYLOAD_SCHEMA.md. Returns dqgA shape: totalIntervals, missingBars, gapCount, maxGapSize, gapRatio, largestGapRatio (no score/verdict)."""
    if not candles or len(candles) < 2 or not timeframe_ms:
        return {
            "totalIntervals": None,
            "missingBars": None,
            "gapCount": None,
            "maxGapSize": None,
            "gapRatio": None,
            "largestGapRatio": None,
        }
    sorted_ts = sorted(
        t for t in (_candle_timestamp(c) for c in candles) if t is not None and isinstance(t, (int, float))
    )
    if len(sorted_ts) < 2:
        return {
            "totalIntervals": None,
            "missingBars": None,
            "gapCount": None,
            "maxGapSize": None,
            "gapRatio": None,
            "largestGapRatio": None,
        }
    time_span = sorted_ts[-1] - sorted_ts[0]
    expected_bars = int(time_span // timeframe_ms) + 1
    total_intervals = max(1, expected_bars - 1)
    total_gaps = 0
    gap_count = 0
    max_consecutive_gap = 0
    for i in range(1, len(sorted_ts)):
        diff = sorted_ts[i] - sorted_ts[i - 1]
        if diff > timeframe_ms:
            missing_bars = int(diff // timeframe_ms) - 1
            total_gaps += missing_bars
            gap_count += 1
            if missing_bars > max_consecutive_gap:
                max_consecutive_gap = missing_bars
    gap_ratio = total_gaps / total_intervals if total_intervals else 0.0
    largest_gap_ratio = max_consecutive_gap / total_intervals if total_intervals else None
    return {
        "totalIntervals": total_intervals,
        "missingBars": total_gaps,
        "gapCount": gap_count,
        "maxGapSize": max_consecutive_gap,
        "gapRatio": round(gap_ratio, 6),
        "largestGapRatio": round(largest_gap_ratio, 6) if largest_gap_ratio is not None else None,
    }


def _raw_price_integrity(candles: list[dict]) -> dict:
    """DQ6 raw only - docs/DQG_RAW_PAYLOAD_SCHEMA.md. Returns dqgB shape: flatBars, flatBarsRatio, optional zeroVolumeBars, etc. (no score/verdict)."""
    if not candles or len(candles) < 100:
        return {
            "flatBars": None,
            "flatBarsRatio": None,
            "zeroVolumeBars": None,
            "zeroVolumeRatio": None,
            "identicalCloseRuns": None,
            "maxIdenticalCloseRun": None,
        }
    flat_bars = 0
    zero_volume_bars = 0
    identical_close_runs = 0
    max_identical_close_run = 0
    run = 0
    prev_close = None
    for bar in candles:
        h = bar.get("h") or bar.get("high")
        l_ = bar.get("l") or bar.get("low")
        o = bar.get("o") or bar.get("open")
        c = bar.get("c") or bar.get("close")
        v = bar.get("v") or bar.get("volume") or 0
        if o is not None and h is not None and l_ is not None and c is not None:
            if o == h == l_ == c:
                flat_bars += 1
        if (v or 0) == 0:
            zero_volume_bars += 1
        if c is not None:
            if prev_close is not None and c == prev_close:
                run += 1
            else:
                if run > max_identical_close_run:
                    max_identical_close_run = run
                run = 1
                prev_close = c
    if run > max_identical_close_run:
        max_identical_close_run = run
    # Count runs of consecutive identical close
    runs_count = 0
    j = 0
    while j < len(candles):
        c = candles[j].get("c") or candles[j].get("close")
        if c is None:
            j += 1
            continue
        k = j + 1
        while k < len(candles) and (candles[k].get("c") or candles[k].get("close")) == c:
            k += 1
        if k > j + 1:
            runs_count += 1
        j = k
    identical_close_runs = runs_count
    n = len(candles)
    flat_bars_ratio = (flat_bars / n) if n else None
    zero_volume_ratio = (zero_volume_bars / n) if n else None
    return {
        "flatBars": flat_bars,
        "flatBarsRatio": round(flat_bars_ratio, 6) if flat_bars_ratio is not None else None,
        "zeroVolumeBars": zero_volume_bars,
        "zeroVolumeRatio": round(zero_volume_ratio, 6) if zero_volume_ratio is not None else None,
        "identicalCloseRuns": identical_close_runs,
        "maxIdenticalCloseRun": max_identical_close_run,
    }


def _log_dqg(msg: str) -> None:
    """Stderr log for DQG/OHLCV debugging (used before _log is defined at module load)."""
    print(f"[DQG] {msg}", file=sys.stderr)


def _try_load_ohlcv(
    user_data: Path,
    exchange: str,
    symbol: str,
    timeframe: str,
    date_from: str,
    date_to: str,
) -> list[dict] | None:
    """Load OHLCV candles for the backtest range. Returns list of {t, o, h, l, c, v} or None."""
    if not exchange or not symbol or not timeframe or not date_from or not date_to:
        _log_dqg("OHLCV load: skip - missing exchange/symbol/timeframe/date_from/date_to")
        return None
    pair_file = _symbol_to_pair_file(symbol)
    datadir = user_data / "data" / exchange.replace(".", "_")
    # OHLCV loading requires Freqtrade (no fallback). Run this script from the Freqtrade venv.
    try:
        from freqtrade.data.history import load_pair_history
    except ImportError as e:
        _log_dqg(f"OHLCV load: Freqtrade not available ({e}). Run from Freqtrade venv if you need OHLCV.")
        return None
    if load_pair_history is not None:
        try:
            pair_slash = pair_file.replace("_", "/")
            try:
                df = load_pair_history(datadir=datadir, pair=pair_slash, timeframe=timeframe)
            except TypeError:
                df = load_pair_history(datadir, pair_slash, timeframe)
            if df is None or df.empty or not hasattr(df, "to_dict"):
                _log_dqg(f"OHLCV load: Freqtrade returned df empty or invalid (df is None={df is None})")
            else:
                rows = df.to_dict("records")
                out = []
                for row in rows:
                    date_val = row.get("date")
                    if date_val is None:
                        continue
                    if hasattr(date_val, "timestamp"):
                        t = int(date_val.timestamp() * 1000)
                    elif isinstance(date_val, (int, float)):
                        t = int(date_val * 1000 if date_val < 1e12 else date_val)
                    else:
                        continue
                    o = row.get("open")
                    h = row.get("high")
                    l_ = row.get("low")
                    c = row.get("close")
                    v = row.get("volume", 0)
                    if o is None and h is None and l_ is None and c is None:
                        continue
                    out.append({"t": t, "o": o, "h": h, "l": l_, "c": c, "v": v or 0})
                if out:
                    try:
                        start_ts = datetime.strptime(date_from[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000
                        end_ts = datetime.strptime(date_to[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000
                        out = [x for x in out if start_ts <= x["t"] <= end_ts]
                    except Exception as e:
                        _log_dqg(f"OHLCV load: date filter error: {e}")
                    if out:
                        return out
                    _log_dqg("OHLCV load: Freqtrade data empty after date range filter")
        except Exception as e:
            _log_dqg(f"OHLCV load: Freqtrade load_pair_history failed: {e!r}")
    _log_dqg("OHLCV load: no data (Freqtrade could not load OHLCV for this range)")
    return None


def _enrich_item_dqg(user_data: Path, item: dict) -> None:
    """If OHLCV is available, set item['dqg'] = { meta, dqgA, dqgB } raw only (no score/verdict). See docs/DQG_RAW_PAYLOAD_SCHEMA.md."""
    bt = item.get("backtestResult") or {}
    cfg = bt.get("config") or {}
    exchange = (cfg.get("exchange") or "").strip() or None
    if isinstance(exchange, dict):
        exchange = exchange.get("name") or exchange.get("key")
    symbol = (cfg.get("symbol") or "").strip() or None
    timeframe = (cfg.get("timeframe") or "").strip()
    date_from = (cfg.get("startDate") or "").strip()[:10]
    date_to = (cfg.get("endDate") or "").strip()[:10]
    if not exchange or not symbol or not timeframe or not date_from or not date_to:
        _log_dqg("DQG enrich: skip - missing config (exchange/symbol/timeframe/startDate/endDate)")
        return
    candles = _try_load_ohlcv(user_data, exchange, symbol, timeframe, date_from, date_to)
    if not candles:
        _log_dqg(
            "DQG enrich: no candles loaded - skipping DQG for this result (upload continues without it). "
            "Run download-data for this pair/timeframe to enable DQG."
        )
        return
    timeframe_ms = _timeframe_to_ms(timeframe)
    sorted_ts = sorted(
        t for t in (_candle_timestamp(c) for c in candles) if t is not None and isinstance(t, (int, float))
    )
    time_span = sorted_ts[-1] - sorted_ts[0] if len(sorted_ts) >= 2 else 0
    candles_expected = int(time_span // timeframe_ms) + 1 if timeframe_ms and time_span else len(candles)
    meta = {
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "dateFrom": f"{date_from}T00:00:00Z" if date_from else None,
        "dateTo": f"{date_to}T23:59:59Z" if date_to else None,
        "candlesExpected": candles_expected,
        "candlesLoaded": len(candles),
        "dataSource": "freqtrade",
        "loadedVia": "freqtrade",
    }
    raw_a = _raw_gap_density(candles, timeframe_ms)
    raw_b = _raw_price_integrity(candles)
    item["dqg"] = {"meta": meta, "dqgA": raw_a, "dqgB": raw_b}


def pair_to_symbol(pair: str) -> str:
    """BTC/USDT -> BTCUSDT. Fallback uses same default as _symbol_to_pair."""
    if not pair:
        return _DEFAULT_PAIR.replace("/", "")
    return pair.replace("/", "").strip().upper()


def _unix_ts_to_ymd(ts: int | float | None) -> str | None:
    """Convert Unix timestamp (seconds) to YYYY-MM-DD. Returns None if invalid."""
    if ts is None:
        return None
    try:
        t = int(ts)
        if t < 0 or t > 2147483647 * 2:  # sanity
            return None
        return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, OSError, OverflowError):
        return None


def timerange_to_dates(
    timerange: str | None = None,
    backtest_start: str | None = None,
    backtest_end: str | None = None,
    backtest_start_ts: int | float | None = None,
    backtest_end_ts: int | float | None = None,
) -> tuple[str, str]:
    """Convert Freqtrade timerange or backtest_start/end or Unix timestamps to YYYY-MM-DD."""
    if backtest_start_ts is not None or backtest_end_ts is not None:
        s = _unix_ts_to_ymd(backtest_start_ts)
        e = _unix_ts_to_ymd(backtest_end_ts)
        if s and e:
            return s, e
        if s:
            return s, e or ""
        if e:
            return "", e
    if timerange and "-" in timerange:
        parts = timerange.strip().split("-")
        if len(parts) == 2:
            s, e = parts[0].strip(), parts[1].strip()
            if len(s) == 8 and len(e) == 8:  # YYYYMMDD
                return f"{s[:4]}-{s[4:6]}-{s[6:8]}", f"{e[:4]}-{e[4:6]}-{e[6:8]}"
    start_ymd = None
    end_ymd = None
    if backtest_start:
        try:
            dt = datetime.fromisoformat(backtest_start.replace("Z", "+00:00"))
            start_ymd = dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    if backtest_end:
        try:
            dt = datetime.fromisoformat(backtest_end.replace("Z", "+00:00"))
            end_ymd = dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    if start_ymd and end_ymd:
        return start_ymd, end_ymd
    if start_ymd:
        return start_ymd, end_ymd or ""
    if end_ymd:
        return "", end_ymd
    return "", ""


def _get_backtest_date_range_from_raw(raw: dict) -> tuple[str, str]:
    """Get (start_ymd, end_ymd) from a raw backtest result (flat or strategy dict)."""
    if not raw:
        return "", ""
    config = raw.get("config") or {}
    timerange = raw.get("timerange") or config.get("timerange")
    backtest_start = raw.get("backtest_start") or config.get("backtest_start")
    backtest_end = raw.get("backtest_end") or config.get("backtest_end")
    backtest_start_ts = raw.get("backtest_start_ts") or config.get("backtest_start_ts")
    backtest_end_ts = raw.get("backtest_end_ts") or config.get("backtest_end_ts")
    strategies = raw.get("strategy")
    if isinstance(strategies, dict):
        for _name, st in strategies.items():
            if isinstance(st, dict):
                backtest_start = backtest_start or st.get("backtest_start")
                backtest_end = backtest_end or st.get("backtest_end")
                backtest_start_ts = backtest_start_ts or st.get("backtest_start_ts")
                backtest_end_ts = backtest_end_ts or st.get("backtest_end_ts")
                break
    return timerange_to_dates(
        timerange=timerange,
        backtest_start=backtest_start,
        backtest_end=backtest_end,
        backtest_start_ts=backtest_start_ts,
        backtest_end_ts=backtest_end_ts,
    )


def _log(msg: str) -> None:
    """Print to stderr (neutral, no internal prefixes)."""
    print(msg, file=sys.stderr)


# --- Terminal UI: new style (Kiploks Integration Bridge only) ---


def _print_header() -> None:
    """Print new-style header (Kiploks Integration Bridge)."""
    print(file=sys.stderr)
    print("Kiploks Integration Bridge v" + str(__version__), file=sys.stderr)
    print("-----------------------------------------", file=sys.stderr)
    print(file=sys.stderr)


def _fmt_pct(decimal_val: float | None, signed: bool = False) -> str:
    """Format decimal (e.g. 0.124) as percentage string (+12.4% or 12.4%). Returns '-' if None."""
    if decimal_val is None:
        return "-"
    pct = float(decimal_val) * 100
    if signed and pct > 0:
        return f"+{pct:.1f}%"
    if signed and pct < 0:
        return f"{pct:.1f}%"
    return f"{pct:.1f}%"


def _print_one_result_block(item: dict, kiploks_config: dict) -> None:
    """Print backtest, equity, WFA and DQG block for a single result (no header, no links)."""
    bt = item.get("backtestResult") or {}
    cfg = bt.get("config") or {}
    res = bt.get("results") or {}
    symbol = (cfg.get("symbol") or "").strip() or "-"
    timeframe = (cfg.get("timeframe") or "").strip() or "-"
    date_from = (cfg.get("startDate") or "")[:10] or "?"
    date_to = (cfg.get("endDate") or "")[:10] or "?"
    total_trades = res.get("totalTrades")
    if total_trades is None and isinstance(bt.get("trades"), list):
        total_trades = len(bt["trades"])
    total_trades = int(total_trades) if total_trades is not None else 0
    total_return = res.get("totalReturn")
    max_dd = res.get("maxDrawdown")
    win_rate = res.get("winRate")
    pf = res.get("profitFactor")
    sharpe = res.get("sharpeRatio")

    print(f"[OK] Backtest loaded: {symbol} | {timeframe} | {date_from} -> {date_to}", file=sys.stderr)
    if max_dd is not None and isinstance(max_dd, (int, float)):
        dd_pct = float(max_dd) * 100
        dd_str = f"-{dd_pct:.1f}%" if dd_pct > 0 else f"{dd_pct:.1f}%"
    else:
        dd_str = "-"
    print(
        f"    Trades: {total_trades} | Return: {_fmt_pct(total_return, signed=True)} | DD: {dd_str}",
        file=sys.stderr,
    )
    pf_str = f"{float(pf):.2f}" if pf is not None else "-"
    sharpe_str = f"{float(sharpe):.2f}" if sharpe is not None else "-"
    print(f"    WinRate: {_fmt_pct(win_rate)} | PF: {pf_str} | Sharpe: {sharpe_str}", file=sys.stderr)
    print(file=sys.stderr)

    equity_curve = bt.get("equityCurve") or []
    n_pts = len(equity_curve) if isinstance(equity_curve, list) else 0
    if n_pts > 0:
        print(f"[OK] Equity curve: reconstructed ({n_pts} pts)", file=sys.stderr)
    else:
        print("[OK] Equity curve: not available", file=sys.stderr)
    print(file=sys.stderr)

    wfa = item.get("walkForwardAnalysis") or {}
    periods = wfa.get("periods") or []
    if periods:
        is_days = int(kiploks_config.get("wfaISSize", 90))
        oos_days = int(kiploks_config.get("wfaOOSSize", 30))
        print(f"[OK] Walk-Forward detected: {len(periods)} windows (IS {is_days}d / OOS {oos_days}d)", file=sys.stderr)
        for i, p in enumerate(periods[:10]):
            is_ret = p.get("optimizationReturn")
            oos_ret = p.get("validationReturn")
            is_t = p.get("isTradesCount")
            oos_t = p.get("oosTradesCount")
            is_ret_s = _fmt_pct(is_ret, signed=True) if is_ret is not None else "-"
            oos_ret_s = _fmt_pct(oos_ret, signed=True) if oos_ret is not None else "-"
            is_ret_s = f"{is_ret_s:>7}"
            oos_ret_s = f"{oos_ret_s:>7}"
            is_t_str = f"{int(is_t)}t" if is_t is not None else "-"
            oos_t_str = f"{int(oos_t)}t" if oos_t is not None else "-"
            is_t_fmt = f"({is_t_str:>4})"
            oos_t_fmt = f"({oos_t_str:>4})"
            print(f"    W{i+1}  IS {is_ret_s} {is_t_fmt} | OOS {oos_ret_s} {oos_t_fmt}", file=sys.stderr)
        if len(periods) > 10:
            print("    ...", file=sys.stderr)
    else:
        print("[OK] Walk-Forward: not detected (single backtest)", file=sys.stderr)
    print(file=sys.stderr)

    dqg = item.get("dqg") or {}
    if dqg:
        meta = dqg.get("meta") or {}
        dqg_a = dqg.get("dqgA") or {}
        dqg_b = dqg.get("dqgB") or {}
        candles_loaded = meta.get("candlesLoaded")
        candles_expected = meta.get("candlesExpected")
        c_loaded = f"{candles_loaded:,}" if candles_loaded is not None else "-"
        c_expected = f"{candles_expected:,}" if candles_expected is not None else "-"
        print("[OK] Data Quality (raw facts)", file=sys.stderr)
        print(f"    Candles: {c_loaded} / {c_expected}", file=sys.stderr)
        gap_count = dqg_a.get("gapCount")
        max_gap = dqg_a.get("maxGapSize")
        flat_ratio = dqg_b.get("flatBarsRatio")
        if gap_count is not None or max_gap is not None or flat_ratio is not None:
            gap_part = (
                f"Gaps: {gap_count} (max {max_gap} bars)"
                if gap_count is not None and max_gap is not None
                else (f"Gaps: {gap_count}" if gap_count is not None else (f"Max gap: {max_gap} bars" if max_gap is not None else ""))
            )
            flat_part = f"Flat bars: {float(flat_ratio) * 100:.2f}%" if flat_ratio is not None else ""
            parts = [p for p in (gap_part, flat_part) if p]
            if parts:
                print("    " + " | ".join(parts), file=sys.stderr)
    else:
        print("[OK] Data Quality: no OHLCV loaded (run download-data to enable)", file=sys.stderr)
    print(file=sys.stderr)


def _print_integration_summary(
    items: list[dict],
    kiploks_config: dict,
    can_upload: bool,
    upload_ok: bool,
    analyze_urls: list[str] | None = None,
    deferred_lines: list[str] | None = None,
    deferred_hyperopt_errors: list[str] | None = None,
) -> None:
    """Print integration summary: each result block first, then any deferred upload/storage lines, then ADVANCED ANALYSIS header and links; hyperopt errors last so success block stays visible."""
    sep = "-----------------------------------------"
    n = len(items)
    urls = analyze_urls or []
    for i, item in enumerate(items):
        if n > 1:
            print(f"--- Result {i + 1}/{n} ---", file=sys.stderr)
            print(file=sys.stderr)
        _print_one_result_block(item, kiploks_config)
        print(file=sys.stderr)
    if deferred_lines:
        for line in deferred_lines:
            print(line, file=sys.stderr)
        print(file=sys.stderr)
    print("ADVANCED ANALYSIS (Kiploks Cloud)", file=sys.stderr)
    print(sep, file=sys.stderr)
    print("DQG verdict, WFE, retention/decay,", file=sys.stderr)
    print("PSI, overfitting probability,", file=sys.stderr)
    print("final deployment grade", file=sys.stderr)
    print(file=sys.stderr)
    show_links = can_upload and upload_ok
    api_url = (kiploks_config.get("api_url") or "").strip()
    api_token = (kiploks_config.get("api_token") or "").strip()
    has_cloud_creds = bool(api_url and api_token)
    if not show_links:
        if not has_cloud_creds:
            print("[ Cloud upload not configured ]", file=sys.stderr)
            print(
                "Set api_url (e.g. https://kiploks.com) and api_token in kiploks.json for Advanced metrics on Kiploks Cloud.",
                file=sys.stderr,
            )
            print("Get a key: https://kiploks.com/api-keys", file=sys.stderr)
        elif not can_upload:
            print("[ Cloud upload skipped ]", file=sys.stderr)
            print("Invalid API credentials - see message above. Advanced metrics need a successful Cloud upload.", file=sys.stderr)
        else:
            print("[ Cloud upload failed ]", file=sys.stderr)
            print(
                "Advanced metrics (DQG, WFE, retention, ...) are computed on Kiploks Cloud after upload succeeds. Fix errors above (HTTP, oos_trades, limits).",
                file=sys.stderr,
            )
    else:
        print("Open the analyze link(s) below to see full analysis.", file=sys.stderr)
    print(file=sys.stderr)
    if show_links and urls:
        for u in urls:
            print(u, file=sys.stderr)
    print(file=sys.stderr)
    # Hyperopt errors at the very end so "[OK] Backtest loaded" and summary remain visible.
    if deferred_hyperopt_errors:
        print(sep, file=sys.stderr)
        print("Hyperopt (errors / warnings)", file=sys.stderr)
        for line in deferred_hyperopt_errors:
            print(line, file=sys.stderr)
        print(file=sys.stderr)
    sys.stderr.flush()


def _print_wfa_progress(
    result_idx: int,
    total_results: int,
    window_idx: int,
    total_windows: int,
    phase: str,
    status: str,
) -> None:
    """Update single progress line during WFA (no block, no duplicate)."""
    step = f"Window {window_idx}/{total_windows}" if total_windows > 1 else "Final"
    _log_progress(f"  Result {result_idx}/{total_results}  {step}  {phase}  {status}")


def _print_wfa_progress_clear() -> None:
    _log_progress_clear()


def _cleanup_created_files() -> None:
    """Remove all files created/renamed by this run (e.g. on KeyboardInterrupt)."""
    for p in _created_by_this_run:
        try:
            if p.exists():
                p.unlink()
                _log(f"Removed (interrupted): {p.name}")
        except OSError:
            pass
    _created_by_this_run.clear()


def _log_progress(msg: str, width: int = 72) -> None:
    """No-op: progress messages disabled (only errors are shown)."""
    pass


def _log_progress_clear() -> None:
    """No-op: progress cleared (only errors are shown)."""
    pass


def _run_wfa_dots_animation(stop_event: threading.Event) -> None:
    """Background thread: cycle 'Running walk-forward analysis.' with moving dots until stop_event is set."""
    base = "Running walk-forward analysis"
    frames = [base + ".  ", base + ".. ", base + "..."]
    idx = 0
    while not stop_event.is_set():
        print(f"\r{frames[idx % 3]}", end="", file=sys.stderr)
        sys.stderr.flush()
        if stop_event.wait(0.4):
            break
        idx += 1


def _is_no_exchange_data_error(stderr: str) -> bool:
    """True if stderr indicates missing OHLCV data (not network or other error)."""
    if not stderr:
        return False
    s = stderr.lower()
    return (
        "no data found" in s
        or "no history for" in s
        or "data ends at" in s
        or "download-data" in s
        or "use `freqtrade download-data`" in s
    )


def _is_exchange_not_available_error(stderr: str) -> bool:
    """True if stderr indicates exchange API unreachable (network/firewall)."""
    if not stderr:
        return False
    s = stderr.lower()
    return (
        "exchangenotavailable" in s
        or "exchange not available" in s
        or "exchangeinfo" in s
        or "could not load markets" in s
    )


def _is_hyperopt_strategy_space_error(stderr: str) -> bool:
    """True if hyperopt failed because strategy has a space (e.g. 'buy' or 'sell') in hyperopt but no parameters for it."""
    if not stderr:
        return False
    s = stderr.lower()
    return (
        "space is included" in s
        and ("no parameter" in s or "remove the" in s)
        and "hyperopt" in s
    )


def _extract_hyperopt_space_name(stderr: str) -> str | None:
    """Extract space name from Freqtrade error e.g. \"The 'buy' space is included...\". Returns 'buy', 'sell', or None."""
    if not stderr:
        return None
    m = re.search(r"The\s+'(\w+)'\s+space\s+is\s+included", stderr, re.IGNORECASE)
    return m.group(1).lower() if m else None


def _log_exchange_not_available_help(exchange: str | None = None) -> None:
    """Log a short hint when backtest fails due to exchange API unreachable."""
    ex = (exchange or "").strip()
    if ex:
        _log(f"Exchange API unreachable. Check network/VPN. Test: docker compose run --rm freqtrade list-pairs --exchange {ex} --quote USDT")
    else:
        _log("Exchange API unreachable. Check network/VPN. Set exchange in config and test list-pairs.")


def _log_download_data_help(
    timerange: str,
    timeframe: str,
    cwd: Path | None,
) -> None:
    """Log a user-friendly message and example command to download exchange data."""
    if cwd:
        project = cwd.resolve()
    else:
        project = Path.cwd()
    mount_path = _path_for_docker_mount(project)
    _log("   • No OHLCV data for this timerange. Download and re-run:")
    _log(f'   •   docker compose run --rm -v "{mount_path}":/app -w /app freqtrade download-data --config /app/user_data/config.json -t {timeframe} --timerange {timerange}')


def _run_hyperopt(
    user_data: Path,
    strategy_name: str,
    timerange: str,
    timeframe: str,
    ft_base: list[str],
    cwd: Path | None,
    userdir_for_cmd: str,
    config_path_in_container: str | None,
    config: dict,
    deferred_error_lines: list[str] | None = None,
) -> tuple[bool, bool]:
    """
    Run freqtrade hyperopt via Docker (expects ft_base with mount). Returns (success, no_data).
    no_data True means failure was due to missing OHLCV data (caller can show download-data help).
    If deferred_error_lines is provided, hyperopt failure messages are appended there instead of logging immediately (so caller can print them at end of run).
    """
    def _emit(msg: str) -> None:
        if deferred_error_lines is not None:
            deferred_error_lines.append(msg)
        else:
            _log(msg)

    if not strategy_name or not timerange:
        return False, False
    hyperopt_loss = (config.get("hyperopt_loss") or "").strip()
    if not hyperopt_loss:
        _log("hyperopt_loss not set in config; skipping hyperopt.")
        return False, False
    epochs = max(3, int(config.get("epochs", 10)))
    args = [
        "hyperopt",
        "--strategy", strategy_name,
        "--hyperopt-loss", hyperopt_loss,
        "--epochs", str(epochs),
        "--timerange", timerange,
        "-i", timeframe,
        "--userdir", userdir_for_cmd,
    ]
    config_exists = bool(_read_freqtrade_config(user_data))
    if config_path_in_container and config_exists:
        args.extend(["--config", config_path_in_container])
    elif not config_exists and config_path_in_container:
        _log("   • user_data/config.json not found; running hyperopt without --config.")
    full_cmd = ft_base + args
    try:
        proc = subprocess.run(
            full_cmd,
            capture_output=True,
            text=True,
            timeout=3600,
            cwd=cwd,
        )
        if proc.returncode == 0:
            time.sleep(1)
            return True, False
        combined = (proc.stderr or "") + "\n" + (proc.stdout or "")
        no_data = _is_no_exchange_data_error(combined)
        if no_data:
            _log_download_data_help(
                timerange=timerange,
                timeframe=timeframe,
                cwd=cwd,
            )
        else:
            err_snippet = combined.strip()
            if len(err_snippet) > 1200:
                err_snippet = err_snippet[-1200:]
            _emit("   • Hyperopt failed. Check strategy name, timerange, and data. Output:")
            _emit("   •   " + err_snippet.replace("\n", "\n   •   "))
            _emit("   • To reproduce: " + " ".join(full_cmd))
            if _is_hyperopt_strategy_space_error(combined):
                space_name = _extract_hyperopt_space_name(combined) or "buy/sell"
                _emit(f"   • Hint: Your strategy has a hyperopt space '{space_name}' with no parameters. Add parameters for that space or remove it from hyperoptimization in your strategy.")
        return False, no_data
    except subprocess.TimeoutExpired:
        _emit("   • Hyperopt timed out (run hyperopt manually with fewer epochs).")
        return False, False
    except Exception as e:
        _emit(f"   • Hyperopt exception: {e}")
        return False, False


def parse_one_backtest_file(path: Path) -> dict | None:
    """Parse a single backtest JSON or first JSON inside a ZIP. Returns raw Freqtrade structure (flat or new format with strategy dict).
    ZIP: only the FIRST matching .json is used (excluding *_config*); other files in the archive are ignored.
    Uses retry on FileNotFoundError to reduce TOCTOU race with Freqtrade.
    Rejects files larger than _MAX_BACKTEST_FILE_SIZE and ZIPs with too many files or too large uncompressed size (DoS protection).
    """
    try:
        if path.stat().st_size > _MAX_BACKTEST_FILE_SIZE:
            _log(f"File too large: {path.name} ({path.stat().st_size} bytes)")
            return None
    except OSError:
        pass  # Missing file etc.; let retry loop below handle FileNotFoundError

    for attempt in range(5):
        try:
            if path.suffix.lower() == ".zip":
                if not zipfile.is_zipfile(path):
                    return None
                with zipfile.ZipFile(path, "r") as zf:
                    total_uncompressed = sum(f.file_size for f in zf.infolist())
                    if total_uncompressed > _MAX_BACKTEST_FILE_SIZE:
                        _log(f"ZIP extracted size too large: {path.name} ({total_uncompressed} bytes)")
                        return None
                    if len(zf.namelist()) > _MAX_ZIP_FILES:
                        _log(f"Too many files in ZIP: {path.name} ({len(zf.namelist())} files)")
                        return None
                    def _safe_zip_member(n: str) -> bool:
                        return n.endswith(".json") and "_config" not in n and ".." not in n and not n.startswith("/") and "\\" not in n
                    names = [n for n in zf.namelist() if _safe_zip_member(n)]
                    if not names:
                        names = [n for n in zf.namelist() if n.endswith(".json") and ".." not in n and not n.startswith("/") and "\\" not in n]
                    if not names:
                        return None
                    with io.TextIOWrapper(zf.open(names[0]), encoding="utf-8") as f:
                        data = json.load(f)
            else:
                content = _read_path_text_with_retry(path)
                if content is None:
                    if attempt < 4:
                        time.sleep(_FILE_READ_RETRY_BASE_SLEEP * (2 ** attempt))
                        continue
                    return None
                data = json.loads(content)
            return data if isinstance(data, dict) else None
        except FileNotFoundError:
            if attempt < 4:
                time.sleep(_FILE_READ_RETRY_BASE_SLEEP * (2 ** attempt))
            else:
                return None
        except (zipfile.BadZipFile, json.JSONDecodeError) as e:
            _log(f"Parse error {path.name}: {e}")
            return None
        except Exception as e:
            _log(f"Parse error {path.name}: {e}")
            return None
    return None


def _validation_max_dd_from_raw(raw: dict) -> float | None:
    """Extract max drawdown from raw OOS backtest result; normalize to decimal. For Professional WFA stress test (periods[].validationMaxDD)."""
    if not raw or not isinstance(raw, dict):
        return None
    v = _first_metric_value(raw, "max_drawdown", "max_relative_drawdown", "max_drawdown_account")
    if v is None and isinstance(raw.get("results"), dict):
        v = _first_metric_value(raw["results"], "max_drawdown", "max_relative_drawdown", "max_drawdown_account")
    if v is None:
        st_list = raw.get("strategy")
        if isinstance(st_list, list) and st_list and isinstance(st_list[0], dict):
            v = _first_metric_value(st_list[0], "max_drawdown", "max_relative_drawdown", "max_drawdown_account")
    if v is None or not isinstance(v, (int, float)) or v != v:
        return None
    v = float(v)
    if v > 1:
        v = v / 100.0
    return v if (v == v and abs(v) < 1e15) else None


def _profit_factor_from_raw(raw: dict) -> float | None:
    """Extract profit_factor from raw backtest result (top-level or results). For WFA Edge Erosion."""
    if not raw or not isinstance(raw, dict):
        return None
    v = raw.get("profit_factor")
    if v is not None and isinstance(v, (int, float)) and v == v:
        return float(v)
    res = raw.get("results")
    if isinstance(res, dict):
        v = res.get("profit_factor")
        if v is not None and isinstance(v, (int, float)) and v == v:
            return float(v)
    return None


def _numeric_params_only(params: dict) -> dict:
    """Keep only numeric (int/float) values for PSI. Excludes 'strategy' and other non-numeric keys."""
    if not params or not isinstance(params, dict):
        return {}
    return {
        k: float(v) if isinstance(v, (int, float)) else v
        for k, v in params.items()
        if isinstance(v, (int, float)) and (v == v and abs(v) != float("inf"))
    }


def _strategy_name_string(parameters: dict) -> str:
    """Ensure strategy name is a string (for Freqtrade -s flag). New format may send a dict."""
    s = parameters.get("strategy")
    if s is None:
        return ""
    if isinstance(s, dict):
        return str(s.get("name") or s.get("class_name") or s.get("strategy_name") or "")
    return str(s)


_STRATEGY_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")


def _validate_strategy_name(name: str) -> str:
    """Validate strategy name before passing to subprocess (Freqtrade -s). Raises ValueError if invalid.
    All subprocess code paths that use strategy_name must call this before building the command."""
    if not _STRATEGY_NAME_RE.match(name):
        raise ValueError(f"Invalid strategy name: {name!r}")
    return name


def _to_return_decimal(pt: object) -> float | None:
    """Convert profit_total / profit_ratio / profit_total_pct to decimal return (e.g. 0.05 = 5%)."""
    if pt is None:
        return None
    try:
        v = float(pt)
    except (TypeError, ValueError):
        return None
    if not (v == v):  # NaN
        return None
    if abs(v) <= 1 and abs(v) != 0:
        return v
    return v / 100.0


def get_profit_total_from_raw(raw: dict) -> float | None:
    """Extract total return (decimal) from raw Freqtrade backtest (flat or strategy dict). Returns None if not found."""
    if not raw:
        return None

    def _try_return(obj: dict | None, keys: list[str]) -> float | None:
        if not obj or not isinstance(obj, dict):
            return None
        for k in keys:
            pt = obj.get(k)
            dec = _to_return_decimal(pt)
            if dec is not None:
                return dec
        return None

    # Nested strategy format: { "strategy": { "StrategyName": { profit_total, results: {...} } } }
    strategies = raw.get("strategy")
    if isinstance(strategies, dict):
        for _name, st in strategies.items():
            if not isinstance(st, dict):
                continue
            dec = _try_return(st, ["profit_total", "profit_ratio", "total_return", "total_return_pct"])
            if dec is not None:
                return dec
            if "profit_total_pct" in st:
                dec = _to_return_decimal(st["profit_total_pct"])
                if dec is not None:
                    return dec
            res = st.get("results")
            dec = _try_return(res, ["profit_total", "profit_ratio", "totalReturn", "total_return"])
            if dec is not None:
                return dec
        return None
    # Top-level or results sub-dict
    results = raw.get("results")
    dec = _try_return(results, ["profit_total", "profit_ratio", "totalReturn", "total_return", "profit_total_pct"])
    if dec is not None:
        return dec
    dec = _try_return(raw, ["profit_total", "profit_ratio", "totalReturn", "total_return", "total_profit"])
    if dec is not None:
        return dec
    if "profit_total_pct" in raw:
        dec = _to_return_decimal(raw["profit_total_pct"])
        if dec is not None:
            return dec
    return None


def _is_strategy_result_object(obj: dict) -> bool:
    """True if obj looks like a per-strategy backtest result (has backtest_start_ts, run_id, or timeframe)."""
    if not obj or not isinstance(obj, dict):
        return False
    return (
        "backtest_start_ts" in obj
        or "backtest_end_ts" in obj
        or ("run_id" in obj and "timeframe" in obj)
    )


@dataclass
class BacktestRun:
    """Internal model for one backtest run. Built from raw file parsing or Freqtrade API."""
    strategy_name: str
    date_from: str
    date_to: str
    symbol: str
    timeframe: str
    params: dict
    total_return: float
    total_trades: int
    win_rate: float
    sharpe_ratio: float | None
    profit_factor: float | None
    max_drawdown: float | None
    trades: list
    config: dict
    equity_curve: list | None


def build_kiploks_item(run: BacktestRun) -> dict:
    """Build Kiploks payload item from internal BacktestRun (same shape as freqtrade_result_to_kiploks output).
    variationHash is not sent; backend computes it from parameters + dateFrom + dateTo."""
    return {
        "parameters": run.params,
        "backtestResult": {
            "config": run.config,
            "results": {
                "totalReturn": run.total_return,
                "winRate": run.win_rate,
                "sharpeRatio": run.sharpe_ratio,
                "profitFactor": run.profit_factor,
                "maxDrawdown": run.max_drawdown,
                "totalTrades": run.total_trades,
            },
            "trades": run.trades,
            "equityCurve": run.equity_curve if run.equity_curve else None,
        },
        "score": run.total_return,
    }


def _raw_run_to_backtest_run(
    raw: dict, strategy_name: str | None = None, initial_balance: float = 1000.0
) -> BacktestRun | None:
    """
    Extract one backtest run from raw Freqtrade dict into BacktestRun.
    Reads metrics from top-level and raw["results"] (nested format). Returns None if raw is empty.
    """
    if not raw:
        return None
    config = raw.get("config") or {}
    timerange = raw.get("timerange") or config.get("timerange")
    backtest_start = raw.get("backtest_start") or config.get("backtest_start")
    backtest_end = raw.get("backtest_end") or config.get("backtest_end")
    backtest_start_ts = raw.get("backtest_start_ts") or config.get("backtest_start_ts")
    backtest_end_ts = raw.get("backtest_end_ts") or config.get("backtest_end_ts")
    date_from, date_to = timerange_to_dates(
        timerange=timerange,
        backtest_start=backtest_start,
        backtest_end=backtest_end,
        backtest_start_ts=backtest_start_ts,
        backtest_end_ts=backtest_end_ts,
    )
    pairs = raw.get("pairlist", []) or config.get("pair_whitelist", []) or []
    pair = (pairs[0] if pairs else config.get("pair")) or ""
    pair = (pair.strip() if isinstance(pair, str) else str(pair).strip()) or ""
    if not pair:
        _log("Backtest result has no pair (pairlist, pair_whitelist, or config.pair). Skipping run.")
        return None
    symbol = pair_to_symbol(pair)
    timeframe = (raw.get("timeframe") or config.get("timeframe") or "").strip()
    if not timeframe:
        _log("Backtest result has no timeframe (raw or config). Skipping run.")
        return None

    params = dict(raw.get("params", raw.get("strategy_params", config.get("strategy_params", {}))))
    if not params:
        params = dict(config.get("strategy_params", {}))
    strategy_str = (strategy_name if isinstance(strategy_name, str) else None) or _strategy_name_string(params) or config.get("strategy") or ""
    params["strategy"] = strategy_str

    def _get_metric(obj: dict, *key_lists: tuple[str, ...]):
        for keys in key_lists:
            for k in keys:
                v = obj.get(k)
                if v is not None and (not isinstance(v, (int, float)) or (v == v)):
                    return v
        res = obj.get("results")
        if isinstance(res, dict):
            for keys in key_lists:
                for k in keys:
                    v = res.get(k)
                    if v is not None and (not isinstance(v, (int, float)) or (v == v)):
                        return v
        return None

    total_return = _finite_float(get_profit_total_from_raw(raw)) or 0.0
    total_trades = int(_get_metric(raw, ("total_trades",)) or 0)
    wins = int(_get_metric(raw, ("wins", "winning_trades")) or 0)
    win_rate = (wins / total_trades) if total_trades else 0.0
    sharpe = _get_metric(raw, ("sharpe_ratio", "sharpe"))
    sharpe = _finite_float(float(sharpe)) if sharpe is not None else None
    profit_factor = _get_metric(raw, ("profit_factor",))
    profit_factor = _finite_float(float(profit_factor)) if profit_factor is not None else None
    max_dd = _get_metric(raw, ("max_drawdown", "max_relative_drawdown", "max_drawdown_account"))
    if max_dd is not None:
        max_dd = float(max_dd)
        if max_dd > 1:
            max_dd = max_dd / 100.0
        max_dd = _finite_float(max_dd)
    else:
        max_dd = None

    trades = raw.get("trades", [])
    equity_curve = []
    if trades:
        balance = initial_balance
        sorted_trades = sorted(trades, key=_trade_date_sort_key)
        for t in sorted_trades:
            try:
                ts = t.get("close_date") or t.get("open_date")
                if not ts:
                    continue
                profit_val = t.get("profit_abs", 0) or t.get("profit") or 0
                balance += float(profit_val)
                equity_curve.append({"timestamp": ts, "equity": balance})
            except (TypeError, ValueError) as e:
                print(
                    f"[backtest] equity_curve: skip invalid trade {t.get('close_date') or t.get('open_date')}: {e}",
                    file=sys.stderr,
                )

    def _exchange_name(obj: dict, key: str = "exchange") -> str | None:
        v = obj.get(key) or obj.get("exchange_name")
        if isinstance(v, dict) and v:
            n = v.get("name")
            if isinstance(n, str) and n.strip():
                return n.strip()
        if isinstance(v, str) and v.strip():
            return v.strip()
        return None

    exchange_name = _exchange_name(raw) or _exchange_name(config) or None
    fee_slippage = _fee_slippage_from_config_and_trades(config, trades)
    strategy_params = config.get("strategy_params") or {}
    config_dict = {
        "symbol": symbol,
        "timeframe": timeframe,
        "startDate": date_from,
        "endDate": date_to,
        "initialBalance": initial_balance,
        **({"exchange": exchange_name} if exchange_name else {}),
        **fee_slippage,
        **({"strategy_params": strategy_params} if strategy_params else {}),
    }

    return BacktestRun(
        strategy_name=strategy_str,
        date_from=date_from,
        date_to=date_to,
        symbol=symbol,
        timeframe=timeframe,
        params=params,
        total_return=total_return,
        total_trades=total_trades,
        win_rate=win_rate,
        sharpe_ratio=sharpe,
        profit_factor=profit_factor,
        max_drawdown=max_dd,
        trades=trades,
        config=config_dict,
        equity_curve=equity_curve if equity_curve else None,
    )


def raw_to_backtest_runs(raw: dict, initial_balance: float = 1000.0) -> list[BacktestRun]:
    """
    Convert raw Freqtrade result (flat or strategy-nested) to list of BacktestRun.
    Same format detection as normalize_raw_to_kiploks_items; returns internal model for API or file-parsing path.
    """
    if not raw or not isinstance(raw, dict):
        return []
    strategies = raw.get("strategy")
    if isinstance(strategies, dict):
        runs = []
        for name, st in strategies.items():
            if not isinstance(st, dict):
                continue
            run = _raw_run_to_backtest_run(st, strategy_name=str(name), initial_balance=initial_balance)
            if run:
                runs.append(run)
        return runs
    if len(raw) >= 1 and "strategy" not in raw and "config" not in raw:
        runs = []
        for key, val in raw.items():
            if isinstance(val, dict) and _is_strategy_result_object(val):
                run = _raw_run_to_backtest_run(val, strategy_name=str(key), initial_balance=initial_balance)
                if run:
                    runs.append(run)
        if runs:
            return runs
    run = _raw_run_to_backtest_run(
        raw,
        strategy_name=raw.get("strategy") if isinstance(raw.get("strategy"), str) else None,
        initial_balance=initial_balance,
    )
    if not run:
        return []
    if run.params.get("strategy") and not isinstance(run.params["strategy"], str):
        run.params["strategy"] = _strategy_name_string(run.params)
    return [run]


def normalize_raw_to_kiploks_items(raw: dict, initial_balance: float = 1000.0) -> list[dict]:
    """
    Convert raw Freqtrade result (flat or new format with strategy dict) to list of Kiploks result items.
    Uses raw_to_backtest_runs + build_kiploks_item. Keeps strategy name override for nested format.
    """
    runs = raw_to_backtest_runs(raw, initial_balance=initial_balance)
    items = []
    for run in runs:
        item = build_kiploks_item(run)
        # Preserve strategy name in parameters when from nested format
        if run.strategy_name and item.get("parameters"):
            item["parameters"]["strategy"] = run.strategy_name
        items.append(item)
    return items


def freqtrade_result_to_kiploks(
    raw: dict, strategy_name: str | None = None, initial_balance: float = 1000.0
) -> dict | None:
    """
    Map one Freqtrade backtest result to Kiploks result item.
    Uses _raw_run_to_backtest_run + build_kiploks_item (reads metrics from top-level and raw["results"]).
    initial_balance: from Freqtrade config dry_run_wallet (strategy run config).
    """
    run = _raw_run_to_backtest_run(raw, strategy_name=strategy_name, initial_balance=initial_balance)
    return build_kiploks_item(run) if run else None


def _first_metric_value(o: dict, *keys: str):
    """Return first non-None value from dict for given keys; for numbers require not NaN. Used when Freqtrade key names vary."""
    for k in keys:
        v = o.get(k)
        if v is not None and (not isinstance(v, (int, float)) or (v == v)):
            return v
    return None


def load_backtest_via_freqtrade_api(
    path: Path, initial_balance: float = 1000.0
) -> list[BacktestRun]:
    """
    Load backtest result using Freqtrade's official API (freqtrade.data.btanalysis).
    Returns list of BacktestRun (one per strategy). Returns [] if Freqtrade is not installed
    or the API raises (e.g. unsupported file format); caller should fall back to file parsing.
    """
    try:
        from freqtrade.data.btanalysis import (load_backtest_data,
                                               load_backtest_stats)
    except ImportError:
        return []

    path_str = str(path.resolve())
    try:
        stats = load_backtest_stats(path_str)
        trades_df = load_backtest_data(path_str)
    except Exception:
        return []

    if not stats or not isinstance(stats, dict):
        return []

    strategies = stats.get("strategy") or {}
    if not isinstance(strategies, dict):
        return []

    # Trades: DataFrame -> list of dicts (snake_case keys for compatibility)
    trades_list = []
    if trades_df is not None and hasattr(trades_df, "to_dict"):
        try:
            trades_list = trades_df.to_dict("records")
        except Exception:
            trades_list = []

    runs = []
    for strategy_name, st in strategies.items():
        if not isinstance(st, dict):
            continue
        # Dates: strategy or top-level
        date_from = (
            st.get("backtest_start")
            or stats.get("backtest_start")
            or ""
        )
        date_to = (
            st.get("backtest_end")
            or stats.get("backtest_end")
            or ""
        )
        if isinstance(date_from, (int, float)) and date_from:
            try:
                date_from = datetime.fromtimestamp(int(date_from) // 1000 if date_from > 1e12 else int(date_from), tz=timezone.utc).strftime("%Y-%m-%d")
            except Exception:
                date_from = str(date_from)[:10] if date_from else ""
        elif not isinstance(date_from, str):
            date_from = str(date_from)[:10] if date_from else ""
        if isinstance(date_to, (int, float)) and date_to:
            try:
                date_to = datetime.fromtimestamp(int(date_to) // 1000 if date_to > 1e12 else int(date_to), tz=timezone.utc).strftime("%Y-%m-%d")
            except Exception:
                date_to = str(date_to)[:10] if date_to else ""
        elif not isinstance(date_to, str):
            date_to = str(date_to)[:10] if date_to else ""

        pairlist = st.get("pairlist") or stats.get("pairlist") or []
        pair_raw = (pairlist[0] if pairlist else stats.get("pair")) or ""
        pair_raw = (pair_raw.strip() if isinstance(pair_raw, str) else str(pair_raw).strip()) or ""
        timeframe = (st.get("timeframe") or stats.get("timeframe") or "").strip()
        if not pair_raw or not timeframe:
            continue
        symbol = pair_to_symbol(pair_raw)

        # Metrics (defensive; keys may vary by Freqtrade version)
        total_return = _first_metric_value(st, "profit_total", "total_return", "profit_total_pct")
        if total_return is not None and isinstance(total_return, (int, float)) and abs(total_return) > 1:
            total_return = total_return / 100.0
        total_return = _finite_float(float(total_return)) if total_return is not None else None
        total_return = total_return if total_return is not None else 0.0

        total_trades = int(_first_metric_value(st, "total_trades") or 0)
        wins = int(_first_metric_value(st, "wins", "winning_trades") or 0)
        win_rate = (wins / total_trades) if total_trades else 0.0

        sharpe = _first_metric_value(st, "sharpe_ratio", "sharpe")
        sharpe = _finite_float(float(sharpe)) if sharpe is not None else None
        profit_factor = _first_metric_value(st, "profit_factor")
        profit_factor = _finite_float(float(profit_factor)) if profit_factor is not None else None

        # Prefer relative drawdown (decimal or %) only; max_drawdown_abs is in USDT and must not be used for results.maxDrawdown
        max_dd = _first_metric_value(st, "max_drawdown", "max_relative_drawdown", "max_drawdown_account")
        if max_dd is not None:
            max_dd = float(max_dd)
            if max_dd > 1:
                max_dd = max_dd / 100.0
            max_dd = _finite_float(max_dd)
        else:
            max_dd = None

        params = dict(st.get("params") or st.get("strategy_params") or {})
        params["strategy"] = str(strategy_name)

        # Equity curve from daily_profit if present
        equity_curve = []
        daily_profit = st.get("daily_profit")
        if isinstance(daily_profit, (list, tuple)) and len(daily_profit) >= 2:
            try:
                balance = initial_balance
                for row in daily_profit:
                    if isinstance(row, (list, tuple)) and len(row) >= 2:
                        date_val, profit_val = row[0], row[1]
                        balance += float(profit_val)
                        ts = date_val
                        if hasattr(ts, "strftime"):
                            ts = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
                        equity_curve.append({"timestamp": ts, "equity": balance})
            except (TypeError, ValueError):
                pass
        if not equity_curve and trades_list:
            balance = initial_balance
            sorted_trades = sorted(trades_list, key=_trade_date_sort_key)
            for t in sorted_trades:
                try:
                    ts = t.get("close_date") or t.get("open_date")
                    if not ts:
                        continue
                    profit_val = t.get("profit_abs", 0) or t.get("profit") or 0
                    balance += float(profit_val)
                    equity_curve.append({"timestamp": ts, "equity": balance})
                except (TypeError, ValueError):
                    pass

        config_dict = {
            "symbol": symbol,
            "timeframe": timeframe,
            "startDate": date_from[:10] if date_from else "",
            "endDate": date_to[:10] if date_to else "",
            "initialBalance": initial_balance,
        }

        runs.append(
            BacktestRun(
                strategy_name=str(strategy_name),
                date_from=date_from[:10] if date_from else "",
                date_to=date_to[:10] if date_to else "",
                symbol=symbol,
                timeframe=timeframe,
                params=params,
                total_return=total_return,
                total_trades=total_trades,
                win_rate=win_rate,
                sharpe_ratio=sharpe,
                profit_factor=profit_factor,
                max_drawdown=max_dd,
                trades=trades_list,
                config=config_dict,
                equity_curve=equity_curve if equity_curve else None,
            )
        )

    return runs


def _load_uploaded_manifest() -> set[str]:
    """Return set of backtest result filenames already uploaded to Kiploks."""
    script_dir = Path(__file__).resolve().parent
    path = script_dir / "uploaded.json"
    if not path.exists():
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        lst = data.get("uploaded") if isinstance(data, dict) else data
        return set(lst) if isinstance(lst, list) else set()
    except (json.JSONDecodeError, OSError):
        return set()


def _save_uploaded_manifest(append_filenames: list[str]) -> None:
    """Append filenames to uploaded.json (dedupe, keep list). Atomic write via temp file + os.replace."""
    script_dir = Path(__file__).resolve().parent
    path = script_dir / "uploaded.json"
    existing = _load_uploaded_manifest()
    existing.update(append_filenames)
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"uploaded": sorted(existing)}, f, indent=2)
        try:
            os.replace(tmp, path)
        except OSError:
            shutil.move(str(tmp), str(path))
    except OSError:
        tmp.unlink(missing_ok=True)


# --- Hyperopt: read .fthypt (JSONL), compute parameter sensitivity for Kiploks ---

def _flatten_params_numeric(obj: dict, prefix: str = "", _depth: int = 0) -> dict[str, float]:
    """Flatten nested dict to {key: number}. Only numeric values; keys like 'buy.ema_short'. Max depth 10 to avoid RecursionError."""
    if _depth > 10:
        return {}
    out: dict[str, float] = {}
    for k, v in obj.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict) and not isinstance(v.get("value"), (int, float)):
            out.update(_flatten_params_numeric(v, key, _depth + 1))
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            out[key] = float(v)
        elif isinstance(v, dict) and "value" in v and isinstance(v.get("value"), (int, float)):
            out[key] = float(v["value"])
    return out


def get_hyperopt_results_dir(user_data: Path) -> Path:
    """Standard Freqtrade hyperopt results directory."""
    return user_data / "hyperopt_results"


def find_latest_fthypt(user_data: Path, config: dict) -> Path | None:
    """Return path to .fthypt file: config['hyperopt_result_path'] or latest by mtime in hyperopt_results/."""
    explicit = config.get("hyperopt_result_path")
    if isinstance(explicit, str) and explicit.strip():
        p = Path(explicit.strip())
        if p.is_file() and p.suffix.lower() == ".fthypt":
            return p
        # Allow path relative to user_data
        rel = user_data / explicit.strip()
        if rel.is_file():
            return rel
    results_dir = get_hyperopt_results_dir(user_data)
    if not results_dir.is_dir():
        return None
    paths = list(results_dir.glob("*.fthypt"))
    if not paths:
        return None
    paths.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return paths[0]


# Max epochs to load from .fthypt JSONL (avoid loading huge files into memory).
_MAX_EPOCHS_JSONL = 50_000
# Max hyperopt trials to send in one upload (avoid huge payloads).
_MAX_HYPEROPT_TRIALS_UPLOAD = 5000


def load_fthypt_epochs(path: Path) -> list[dict]:
    """Read .fthypt JSONL: one JSON object per line (one epoch per line). Return list of epoch dicts.
    Uses _read_path_text_with_retry to reduce TOCTOU race. Stops after _MAX_EPOCHS_JSONL lines."""
    content = _read_path_text_with_retry(path)
    if content is None:
        return []
    epochs: list[dict] = []
    for line in content.splitlines():
        if len(epochs) >= _MAX_EPOCHS_JSONL:
            break
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
            if isinstance(data, dict):
                epochs.append(data)
        except json.JSONDecodeError:
            continue
    return epochs


def build_data_points_from_epochs(epochs: list[dict]) -> list[dict]:
    """Build list of {parameters: {name: number}, score: float} for sensitivity. Score = -loss or profit_total."""
    data_points: list[dict] = []
    for ep in epochs:
        params_raw = ep.get("params_dict") or ep.get("params_details") or {}
        if isinstance(params_raw, dict):
            flat = _flatten_params_numeric(params_raw)
        else:
            flat = {}
        if not flat:
            continue
        loss = ep.get("loss")
        if isinstance(loss, (int, float)):
            score = float(-loss)
        else:
            metrics = ep.get("results_metrics") or {}
            score = float(metrics.get("profit_total", 0) or 0)
        data_points.append({"parameters": flat, "score": score})
    return data_points


# --- Pydantic models for Freqtrade data validation (different versions = different structures) ---

if _PYDANTIC_AVAILABLE:

    class WFAPeriodReturns(BaseModel):
        """Validated optimization and validation return for one WFA period (finite numbers only)."""

        optimization_return: float
        validation_return: float

        @field_validator("optimization_return", "validation_return")
        @classmethod
        def _finite(cls, v: float) -> float:
            if v != v or abs(v) == float("inf"):
                raise ValueError("Return must be finite")
            return v

    class WFAWindow(BaseModel):
        """Validated WFA window from Freqtrade wfa_results (various key names across versions)."""

        start_date: str
        end_date: str
        optimization_return: float | None = None
        validation_return: float | None = None

        @field_validator("optimization_return", "validation_return")
        @classmethod
        def _finite_or_none(cls, v: float | None) -> float | None:
            if v is None:
                return None
            if v != v or abs(v) == float("inf"):
                raise ValueError("Return must be finite or None")
            return v

        @model_validator(mode="before")
        @classmethod
        def _from_freqtrade_keys(cls, data: object) -> object:
            if not isinstance(data, dict):
                return data
            start = (
                data.get("train_start")
                or data.get("start")
                or data.get("in_sample_start")
            )
            end = (
                data.get("test_end")
                or data.get("end")
                or data.get("out_of_sample_end")
            )
            opt = (
                data.get("optimization_return")
                or data.get("train_return")
                or data.get("in_sample_return")
            )
            val = (
                data.get("validation_return")
                or data.get("test_return")
                or data.get("out_of_sample_return")
            )
            start = start[:10] if isinstance(start, str) and len(start) >= 10 else (start if isinstance(start, str) else "")
            end = end[:10] if isinstance(end, str) and len(end) >= 10 else (end if isinstance(end, str) else "")
            try:
                opt_f = float(opt) if opt is not None else None
            except (TypeError, ValueError):
                opt_f = None
            try:
                val_f = float(val) if val is not None else None
            except (TypeError, ValueError):
                val_f = None
            return {
                "start_date": start or "",
                "end_date": end or "",
                "optimization_return": opt_f,
                "validation_return": val_f,
            }


def scan_backtest_results(user_data: Path, top_n: int) -> list[dict]:
    """Scan backtest_results/, parse files (flat or new format), sort by date from filename (freshest first), return top_n as Kiploks items.
    Skips files listed in uploaded.json when skip_already_uploaded is true in config. Attaches _source_file to each item."""
    results_dir = user_data / "backtest_results"
    if not results_dir.is_dir():
        return []
    config = load_config()
    skip_uploaded = config.get("skip_already_uploaded", True)
    uploaded_set = _load_uploaded_manifest() if skip_uploaded else set()
    initial_balance = get_initial_balance_from_freqtrade_config(user_data)
    exchange_from_config = get_exchange_from_freqtrade_config(user_data)
    candidates = []
    resolved_dir = results_dir.resolve()
    paths = list(results_dir.glob("*.json")) + list(results_dir.glob("*.zip"))
    paths = [p for p in paths if not p.name.startswith(".") and "_config" not in p.name and "_meta" not in p.name and not p.name.endswith(".meta.json")]
    paths = [p for p in paths if _path_under(p, resolved_dir)]
    if skip_uploaded:
        paths = [p for p in paths if p.name not in uploaded_set]
    for path in paths:
        balance = get_initial_balance_from_backtest_result_path(path)
        use_balance = balance if balance is not None else initial_balance
        exchange_from_path = get_exchange_from_backtest_result_path(path)
        effective_exchange = exchange_from_path or exchange_from_config
        file_date = _date_from_backtest_filename(path)
        try:
            mtime = path.stat().st_mtime
        except (FileNotFoundError, OSError):
            # File list can change while scanning (docker mount sync / concurrent writes).
            # Skip vanished files instead of failing the whole integration run.
            _log(f"Skipped disappeared backtest file: {path.name}")
            continue
        embedded_config = _read_config_from_backtest_path(path)
        fee_from_embedded = _fee_slippage_from_config_and_trades(embedded_config, None) if embedded_config else {}

        # Prefer Freqtrade official API when available (format-agnostic).
        # Skip API for kiploks-*.zip: we renamed the file but zip still has original member name (e.g. backtest-result-*.json), so API would fail with "file not found in zip".
        use_api = not (path.suffix.lower() == ".zip" and path.name.startswith(KIPLOKS_BACKTEST_PREFIX))
        runs = load_backtest_via_freqtrade_api(path, initial_balance=use_balance) if use_api else []
        if runs:
            for run in runs:
                item = build_kiploks_item(run)
                if item.get("parameters"):
                    item["parameters"]["strategy"] = run.strategy_name
                bt = item.get("backtestResult") or {}
                cfg = dict(bt.get("config") or {})
                if effective_exchange and not cfg.get("exchange"):
                    cfg["exchange"] = effective_exchange
                if fee_from_embedded:
                    for k, v in fee_from_embedded.items():
                        if k not in cfg:
                            cfg[k] = v
                item["backtestResult"] = {**bt, "config": cfg}
                item["_source_file"] = path.name
                _enrich_item_dqg(user_data, item)
                candidates.append(((file_date, mtime), item))
            continue

        # Legacy: parse JSON/ZIP directly
        raw = parse_one_backtest_file(path)
        if not raw:
            continue
        items = normalize_raw_to_kiploks_items(raw, initial_balance=use_balance)
        for item in items:
            if item:
                bt = item.get("backtestResult") or {}
                cfg = dict(bt.get("config") or {})
                if effective_exchange and not cfg.get("exchange"):
                    cfg["exchange"] = effective_exchange
                if fee_from_embedded:
                    for k, v in fee_from_embedded.items():
                        if k not in cfg:
                            cfg[k] = v
                item["backtestResult"] = {**bt, "config": cfg}
                item["_source_file"] = path.name
                _enrich_item_dqg(user_data, item)
                sort_key = (file_date, mtime)
                candidates.append((sort_key, item))
    candidates.sort(key=lambda x: x[0], reverse=True)
    out = [item for _, item in candidates[: top_n]]
    return out


def prune_kiploks_backtest_files(user_data: Path, keep_n: int) -> None:
    """Keep only the keep_n newest files that we created (kiploks-backtest-result*). keep_n=0 removes all. User files are never deleted."""
    if keep_n < 0:
        return
    results_dir = user_data / "backtest_results"
    if not results_dir.is_dir():
        return
    resolved_dir = results_dir.resolve()
    paths = [
        p for p in list(results_dir.glob("*.zip")) + list(results_dir.glob("*.json"))
        if p.name.startswith(KIPLOKS_BACKTEST_PREFIX)
        and not p.name.endswith(".meta.json")
        and "_config" not in p.name
        and _path_under(p, resolved_dir)
    ]
    if len(paths) == 0:
        return
    by_mtime = sorted(paths, key=lambda x: x.stat().st_mtime, reverse=True)
    to_remove = by_mtime[keep_n:] if keep_n > 0 else by_mtime
    for p in to_remove:
        try:
            p.unlink()
            for suffix in (".meta.json", ".meta"):
                meta = p.parent / (p.stem + suffix)
                if meta.exists():
                    try:
                        meta.unlink()
                    except OSError:
                        pass
        except OSError:
            pass


def has_wfa_results(user_data: Path) -> bool:
    """Check if wfa_results/ exists and contains at least one WFA structure."""
    wfa_dir = user_data / "wfa_results"
    if not wfa_dir.is_dir():
        return False
    for path in wfa_dir.rglob("*.json"):
        try:
            content = _read_path_text_with_retry(path)
            if content is None:
                continue
            data = json.loads(content)
            if isinstance(data, dict) and (data.get("windows") or data.get("walk_forward_windows")):
                return True
            if isinstance(data, list) and len(data) > 0:
                return True
        except json.JSONDecodeError:
            continue
        except (OSError, FileNotFoundError):
            continue
    return False


def _parse_wfa_window_dict(w: dict) -> dict:
    """
    Parse a single WFA window dict (Freqtrade keys vary by version). Same key resolution as WFAWindow._from_freqtrade_keys.
    Returns dict with start_date, end_date, optimization_return, validation_return (all normalized).
    """
    start = (
        w.get("train_start")
        or w.get("start")
        or w.get("in_sample_start")
    )
    end = (
        w.get("test_end")
        or w.get("end")
        or w.get("out_of_sample_end")
    )
    opt = (
        w.get("optimization_return")
        or w.get("train_return")
        or w.get("in_sample_return")
    )
    val = (
        w.get("validation_return")
        or w.get("test_return")
        or w.get("out_of_sample_return")
    )
    start = start[:10] if isinstance(start, str) and len(start) >= 10 else (start if isinstance(start, str) else "")
    end = end[:10] if isinstance(end, str) and len(end) >= 10 else (end if isinstance(end, str) else "")
    try:
        opt_f = float(opt) if opt is not None else None
    except (TypeError, ValueError):
        opt_f = None
    try:
        val_f = float(val) if val is not None else None
    except (TypeError, ValueError):
        val_f = None
    return {
        "start_date": start or "",
        "end_date": end or "",
        "optimization_return": opt_f,
        "validation_return": val_f,
    }


def collect_wfa_from_folder(user_data: Path) -> list[dict]:
    """If wfa_results/ has data, load and merge into Kiploks results (add walkForwardAnalysis)."""
    wfa_dir = user_data / "wfa_results"
    if not wfa_dir.is_dir():
        return []
    out = []
    for path in sorted(wfa_dir.rglob("*.json"))[:10]:
        try:
            content = _read_path_text_with_retry(path)
            if content is None:
                _log(f"   • WFA file unreadable: {path.name}")
                continue
            data = json.loads(content)
        except json.JSONDecodeError as e:
            _log(f"   • WFA file invalid JSON: {path.name}: {e}")
            continue
        windows = data.get("windows") or data.get("walk_forward_windows") or (data if isinstance(data, list) else [])
        if not windows:
            continue
        periods = []
        for idx, w in enumerate(windows):
            if not isinstance(w, dict):
                _log(f"   • WFA window[{idx}] in {path.name} skipped: not a dict")
                continue
            if _PYDANTIC_AVAILABLE:
                try:
                    win = WFAWindow.model_validate(w)
                except PydanticValidationError as e:
                    _log(f"   • WFA window[{idx}] in {path.name} validation failed: {e}")
                    continue
                if not win.start_date or not win.end_date:
                    continue
                w_params = _numeric_params_only(w.get("params") or w.get("parameters") or {})
                periods.append({
                    "startDate": win.start_date[:10] if len(win.start_date) >= 10 else win.start_date,
                    "endDate": win.end_date[:10] if len(win.end_date) >= 10 else win.end_date,
                    "optimizationReturn": win.optimization_return,
                    "validationReturn": win.validation_return,
                    **({"parameters": w_params} if w_params else {}),
                })
            else:
                parsed = _parse_wfa_window_dict(w)
                if parsed["start_date"] and parsed["end_date"]:
                    w_params = _numeric_params_only(w.get("params") or w.get("parameters") or {})
                    periods.append({
                        "startDate": parsed["start_date"][:10] if len(parsed["start_date"]) >= 10 else parsed["start_date"],
                        "endDate": parsed["end_date"][:10] if len(parsed["end_date"]) >= 10 else parsed["end_date"],
                        "optimizationReturn": parsed["optimization_return"],
                        "validationReturn": parsed["validation_return"],
                        **({"parameters": w_params} if w_params else {}),
                    })
        if periods:
            out.append({"periods": periods, "source_file": str(path)})
    return out


def _running_inside_container() -> bool:
    """True if we're inside Docker/Podman so we must use python -m freqtrade, not the CLI wrapper. Heuristic."""
    if Path("/.dockerenv").exists():
        return True
    if Path("/run/.containerenv").exists():
        return True  # Podman
    if os.environ.get("container") is not None:
        return True  # Docker/Podman often set this
    try:
        cgroup = Path("/proc/1/cgroup").read_text()
        if "docker" in cgroup or "containerd" in cgroup:
            return True
    except OSError:
        pass
    return False


# Allowlist for each token after shlex.split (command injection prevention). No shell metacharacters.
_SAFE_CMD_TOKEN_RE = re.compile(r"^[A-Za-z0-9_/\\.:=-]+$")
# Allowed first token (executable basename) for FREQTRADE_CMD. Paths like /venv/bin/freqtrade are allowed via basename.
_ALLOWED_FREQTRADE_FIRST_TOKENS = frozenset({"freqtrade", "python", "python3", "docker", "docker-compose"})


def _parse_freqtrade_cmd(cmd_str: str) -> list[str]:
    """Parse FREQTRADE_CMD with shlex.split(); validate each token with allowlist (no denylist).
    Raises ValueError if cmd_str is unsafe. Never use shell=True with the result."""
    cmd_str = cmd_str.strip()
    if not cmd_str:
        return []
    parts = shlex.split(cmd_str)
    if not parts:
        return []
    for i, token in enumerate(parts):
        if not _SAFE_CMD_TOKEN_RE.match(token):
            raise ValueError(f"FREQTRADE_CMD token contains invalid characters: {token[:50]!r}")
    first_base = os.path.basename(parts[0]).lower()
    if first_base not in _ALLOWED_FREQTRADE_FIRST_TOKENS:
        raise ValueError(
            f"FREQTRADE_CMD must start with one of {sorted(_ALLOWED_FREQTRADE_FIRST_TOKENS)}; got {parts[0]!r}"
        )
    return parts


def _resolve_freqtrade_cmd() -> list[str]:
    """FREQTRADE_CMD env required when not running inside container.
    When running inside a container, use same interpreter as this script: sys.executable -m freqtrade
    (the CLI wrapper in the image fails with ModuleNotFoundError).
    Parsing uses shlex.split() and whitelist to prevent command injection; subprocess is always called without shell."""
    if _running_inside_container():
        return [sys.executable, "-m", "freqtrade"]
    raw = (os.environ.get("FREQTRADE_CMD") or "").strip()
    if not raw:
        _log("FREQTRADE_CMD is not set. Set it to your freqtrade invocation (e.g. 'docker compose run --rm freqtrade').")
        sys.exit(1)
    try:
        argv = _parse_freqtrade_cmd(raw)
        if argv:
            _log("Freqtrade argv (audit): " + " ".join(argv))
            return argv
    except ValueError as e:
        _log(f"FREQTRADE_CMD rejected: {e}")
        sys.exit(1)
    sys.exit(1)


def _path_for_docker_mount(path: Path) -> str:
    """Path string for Docker -v (host side). On Windows, Docker (WSL2) expects /mnt/c/... not C:\\...."""
    resolved = path.resolve()
    posix = resolved.as_posix()
    if platform.system() == "Windows" and len(posix) >= 2 and posix[1] == ":":
        # C:/Users/... -> /mnt/c/Users/...
        return "/mnt/" + posix[0].lower() + posix[2:]
    return posix


def _docker_cmd_with_project_mount(ft_base: list[str], repo_root: Path) -> list[str]:
    """Inject -v repo_root:/app -w /app so the container writes to host user_data/backtest_results.
    Use /app to avoid overwriting /freqtrade in the image (where the freqtrade package lives).
    On Windows, repo_root is converted to /mnt/c/... for Docker (WSL2) compatibility."""
    host_path = _path_for_docker_mount(repo_root)
    mount = ["-v", f"{host_path}:/app", "-w", "/app"]
    out = []
    for part in ft_base:
        out.append(part)
        if part == "--rm":
            out.extend(mount)
    # Ensure service name (e.g. freqtrade) stays after options; if we didn't find --rm, insert before last token
    if "-w" not in out and len(out) >= 1:
        out = out[:-1] + mount + out[-1:]
    return out


def _timeframe_from_item(item: dict) -> str:
    """Get timeframe from backtestResult.config (e.g. 5m, 1d). Returns empty string if not set."""
    cfg = (item.get("backtestResult") or {}).get("config") or {}
    return (cfg.get("timeframe") or "").strip()


def _check_freqtrade_runnable(ft_base: list[str], cwd: Path | None = None) -> bool:
    """Run freqtrade --version; return True if success. Use cwd so docker compose finds yml."""
    try:
        proc = subprocess.run(
            ft_base + ["--version"],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=cwd,
        )
        if proc.returncode == 0:
            return True
        stderr = (proc.stderr or proc.stdout or "").strip()[:200]
        _log(f"freqtrade not runnable: {stderr}")
    except Exception as e:
        _log(f"freqtrade not runnable: {e}")
    _log("To get WFA: run this script with the same command you use for freqtrade (e.g. activate your env first), or set FREQTRADE_CMD.")
    return False


def _backtest_run_to_raw_like(run: BacktestRun) -> dict:
    """Build a minimal raw-like dict from BacktestRun for WFA code that expects raw (date range, trades, profit_total)."""
    return {
        "backtest_start": run.date_from,
        "backtest_end": run.date_to,
        "trades": run.trades,
        "profit_total": run.total_return,
    }


def _get_trades_from_raw(raw: dict) -> list:
    """Extract trades list from raw backtest result (flat or strategy dict format)."""
    if not raw:
        return []
    if isinstance(raw.get("trades"), list):
        return raw["trades"]
    strategies = raw.get("strategy")
    if isinstance(strategies, dict):
        for _name, st in strategies.items():
            if isinstance(st, dict) and isinstance(st.get("trades"), list):
                return st["trades"]
    return []


def _trade_timestamp_to_iso(ts: object) -> str | None:
    """Convert trade timestamp (str, datetime, or number) to ISO date string for Kiploks."""
    if ts is None:
        return None
    try:
        if hasattr(ts, "isoformat"):
            date_str = ts.isoformat()
        elif isinstance(ts, (int, float)):
            date_str = datetime.fromtimestamp(
                int(ts) / 1000 if ts > 1e12 else int(ts),
                tz=timezone.utc,
            ).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        elif isinstance(ts, str):
            if "T" in ts:
                date_str = ts.replace(" ", "T") if "T" not in ts else ts
            else:
                date_str = ts + "T00:00:00.000Z" if len(ts) <= 10 else ts
        else:
            date_str = str(ts)
        if not date_str.endswith("Z") and "+" not in date_str:
            date_str = date_str + "Z"
        return date_str
    except Exception:
        return None


def _freqtrade_trades_to_oos_trades(
    trades_list: list[dict],
    initial_balance: float,
) -> tuple[list[dict], float]:
    """
    Convert Freqtrade OOS trades to Kiploks oos_trades format (net_return decimal per trade).
    Returns (list of {"net_return": float}, balance_after) for chaining across windows.
    Uses profit_ratio or close_profit if present, else profit_abs / running_balance.
    """
    out: list[dict] = []
    balance = float(initial_balance)

    def _sort_key(x: dict) -> str:
        ts = x.get("close_date") or x.get("open_date") or x.get("close_timestamp") or x.get("open_timestamp") or 0
        if isinstance(ts, (int, float)):
            try:
                sec = int(ts) / 1000 if ts > 1e12 else int(ts)
                return datetime.fromtimestamp(sec, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            except (OSError, OverflowError, ValueError):
                return "0000-00-00"
        if hasattr(ts, "isoformat"):
            return ts.isoformat()
        return str(ts)

    for t in sorted(trades_list, key=_sort_key):
        profit_abs = float(t.get("profit_abs", 0) or t.get("profit") or 0)
        profit_ratio = t.get("profit_ratio") if t.get("profit_ratio") is not None else t.get("close_profit")
        if profit_ratio is not None:
            try:
                net = float(profit_ratio)
            except (TypeError, ValueError):
                net = (profit_abs / balance) if balance else 0.0
        else:
            net = (profit_abs / balance) if balance else 0.0
        if not (isinstance(net, (int, float)) and net == net):  # NaN check
            continue
        out.append({"net_return": net})
        balance += profit_abs
    return out, balance


def _trades_to_equity_curve_kiploks(
    trades: list,
    initial_balance: float = 1000.0,
    max_points: int = 200,
) -> list[dict]:
    """
    Build Kiploks-format equity curve from trades: [{ "date": "<ISO>", "value": number }].
    Downsample to at most max_points points.
    Accepts close_date/open_date as string, datetime, or timestamp (ms/s).
    """
    out = []
    balance = initial_balance

    def _sort_key(x: dict) -> str:
        """Normalize to ISO string so mixed int/str timestamps sort without TypeError."""
        ts = x.get("close_date") or x.get("open_date") or x.get("close_timestamp") or x.get("open_timestamp") or 0
        if isinstance(ts, (int, float)):
            try:
                sec = int(ts) / 1000 if ts > 1e12 else int(ts)
                return datetime.fromtimestamp(sec, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            except (OSError, OverflowError, ValueError):
                return "0000-00-00"
        if hasattr(ts, "isoformat"):
            return ts.isoformat()
        return str(ts)

    sorted_trades = sorted(trades, key=_sort_key)
    for t in sorted_trades:
        ts = t.get("close_date") or t.get("open_date") or t.get("close_timestamp") or t.get("open_timestamp")
        date_str = _trade_timestamp_to_iso(ts)
        if date_str:
            if not out:
                out.append({"date": date_str, "value": round(balance, 2)})
            balance += float(t.get("profit_abs", 0) or t.get("profit") or 0)
            out.append({"date": date_str, "value": round(balance, 2)})
    if len(out) <= max_points:
        return out
    step = max(1, len(out) // max_points)
    return [out[i] for i in range(0, len(out), step)][:max_points]


def _run_backtest_for_timerange(
    user_data: Path,
    strategy_name: str,
    start_str: str,
    end_str: str,
    ft_base: list[str],
    userdir_for_cmd: str,
    cwd: Path | None,
    timeframe: str = "5m",
    max_points: int = 200,
    initial_balance: float = 1000.0,
    config_path_in_container: str | None = None,
) -> tuple[dict | None, list[dict], float | None]:
    """
    Run Freqtrade backtest for the given timerange. Uses same timeframe as downloaded data (-i).
    Results are read from user_data/backtest_results on host. Returns (raw_result, equity_curve_kiploks, total_return).
    """
    _validate_strategy_name(strategy_name)
    timerange = datetime.strptime(start_str[:10], "%Y-%m-%d").strftime("%Y%m%d") + "-" + datetime.strptime(end_str[:10], "%Y-%m-%d").strftime("%Y%m%d")
    run_start_time = datetime.now(timezone.utc).timestamp()
    backtest_args = [
        "backtesting",
        "--userdir", userdir_for_cmd,
        "--timerange", timerange,
        "-i", timeframe,
        "--export", "trades",  # must save result so we can read it from user_data/backtest_results
    ]
    if config_path_in_container:
        backtest_args.extend(["--config", config_path_in_container])
    backtest_args.extend(["-s", strategy_name])
    try:
        proc = subprocess.run(
            ft_base + backtest_args,
            capture_output=True,
            text=True,
            timeout=300,
            cwd=cwd,
        )
        if proc.returncode != 0:
            combined = (proc.stderr or "") + "\n" + (proc.stdout or "")
            _log(f"Backtest failed (exit {proc.returncode})")
            if _is_no_exchange_data_error(combined):
                _log_download_data_help(
                    timerange=timerange,
                    timeframe=timeframe,
                    cwd=cwd,
                )
            elif _is_exchange_not_available_error(combined):
                _log_exchange_not_available_help(get_exchange_from_freqtrade_config(user_data))
            return None, [], 0.0
        # Wait so host filesystem sees new file (Docker mount sync, especially macOS)
        time.sleep(2)
    except Exception as e:
        _log(f"  backtest exception: {e}")
        return None, [], 0.0
    def _is_real_backtest_file(p: Path) -> bool:
        n = p.name
        if n.startswith(".") or n.endswith(".meta.json") or "_config" in n or "_meta" in n or "last_result" in n:
            return False
        return True

    want_start = start_str[:10] if len(start_str) >= 10 else ""
    want_end = end_str[:10] if len(end_str) >= 10 else ""
    bt_dir = user_data / "backtest_results"
    newest = None
    used_path = None
    if bt_dir.is_dir():
        # Snapshot file list to avoid TOCTOU when iterating (Freqtrade may add/remove files).
        bt_resolved = bt_dir.resolve()
        all_candidates = [
            p for p in
            [p for p in bt_dir.glob("*.json") if _is_real_backtest_file(p)]
            + [p for p in bt_dir.glob("*.zip") if _is_real_backtest_file(p)]
            if _path_under(p, bt_resolved)
        ]
        now_ts = time.time()
        recent_cutoff = now_ts - 120
        # Prefer file written by this run (mtime after we started) so we never use an old cached file
        run_cutoff = run_start_time - 3
        matching: list[tuple[Path, dict, float]] = []
        for p in all_candidates:
            raw = None
            # Skip API for kiploks-*.zip (zip member name does not match; use file parser)
            use_api = not (p.suffix.lower() == ".zip" and p.name.startswith(KIPLOKS_BACKTEST_PREFIX))
            if use_api:
                runs = load_backtest_via_freqtrade_api(p, initial_balance=initial_balance)
                if runs:
                    raw = _backtest_run_to_raw_like(runs[0])
            if not raw:
                raw = parse_one_backtest_file(p)
            if not raw:
                continue
            res_start, res_end = _get_backtest_date_range_from_raw(raw)
            if want_start and want_end and res_start == want_start and res_end == want_end:
                try:
                    mtime = p.stat().st_mtime
                except OSError:
                    continue
                matching.append((p, raw, mtime))
        if matching:
            from_this_run = [(p, r, m) for p, r, m in matching if m >= run_cutoff]
            if from_this_run:
                used_path, newest, _ = max(from_this_run, key=lambda x: x[2])
            else:
                # Do not use older files with same date range (could be from previous run)
                _log("  no result file from this run (mtime >= run start); same-date file from earlier run - re-run backtest or remove old files")
                return None, [], None
        if newest is None:
            def _safe_st_mtime(px: Path) -> float:
                try:
                    return px.stat().st_mtime
                except OSError:
                    return 0.0
            by_mtime = sorted(all_candidates, key=_safe_st_mtime, reverse=True)
            for p in by_mtime:
                use_api = not (p.suffix.lower() == ".zip" and p.name.startswith(KIPLOKS_BACKTEST_PREFIX))
                if use_api:
                    runs = load_backtest_via_freqtrade_api(p, initial_balance=initial_balance)
                    if runs:
                        newest = _backtest_run_to_raw_like(runs[0])
                if not newest:
                    newest = parse_one_backtest_file(p)
                if newest:
                    used_path = p
                    break
    if not newest:
        _log(f"  no result file found in {bt_dir} (backtest may have written elsewhere or failed)")
        return None, [], 0.0
    # Mark as ours so we can prune only these later (user files stay untouched)
    if used_path and not used_path.name.startswith(KIPLOKS_BACKTEST_PREFIX):
        old_stem = used_path.stem
        new_name = "kiploks-" + used_path.name
        new_path = used_path.parent / new_name
        created_this_run = used_path.stat().st_mtime >= (time.time() - 120)
        try:
            try:
                os.rename(str(used_path), str(new_path))
            except OSError:
                shutil.move(str(used_path), str(new_path))
            used_path = new_path
            if created_this_run:
                _created_by_this_run.append(new_path)
            old_meta = used_path.parent / (old_stem + ".meta.json")
            if old_meta.exists():
                new_meta = used_path.parent / (new_path.stem + ".meta.json")
                try:
                    os.rename(str(old_meta), str(new_meta))
                except OSError:
                    shutil.move(str(old_meta), str(new_meta))
                if created_this_run:
                    _created_by_this_run.append(new_meta)
        except OSError as e:
            _log(f"  could not rename to {new_name}: {e}")
    result_start, result_end = _get_backtest_date_range_from_raw(newest)
    if want_start and want_end and (result_start != want_start or result_end != want_end):
        _log(f"WARNING: result range {result_start}..{result_end} != requested {want_start}..{want_end}")
    trades = _get_trades_from_raw(newest)
    ret = get_profit_total_from_raw(newest)
    curve = _trades_to_equity_curve_kiploks(trades, initial_balance=initial_balance, max_points=max_points)
    return newest, curve, ret  # ret may be None if profit_total not found in result


def generate_wfa_for_top(
    user_data: Path, top_n: int, config: dict, prefetched: list[dict] | None = None
) -> list[dict]:
    """
    Generate WFA to match frontend optimization: same params (periods, isSize, oosSize in days).
    requiredDays = (isSize + oosSize) * periods; each window = IS then OOS. Two backtests per window.
    Builds walkForwardAnalysis.periods and performanceTransfer (equity curves). 100 points per curve (frontend default).
    If prefetched is provided and non-empty, use it instead of scanning backtest_results again.
    """
    backtest_results = prefetched if prefetched else scan_backtest_results(user_data, top_n)
    if not backtest_results:
        _log("No results to process")
        return []
    wfa_periods_config = config.get("wfaPeriods")
    use_fixed_periods = False
    fixed_periods = None
    if wfa_periods_config is not None and str(wfa_periods_config).strip() != "":
        try:
            n = int(wfa_periods_config)
            if n > 0:
                use_fixed_periods = True
                fixed_periods = max(1, n)
        except (TypeError, ValueError):
            pass
    is_size_days = int(config.get("wfaISSize", 90))
    oos_size_days = int(config.get("wfaOOSSize", 30))
    is_size_days = max(1, is_size_days)
    oos_size_days = max(1, oos_size_days)
    total_window_days = is_size_days + oos_size_days
    points_per_curve = 100
    repo_root = get_repo_root()
    ft_base = _resolve_freqtrade_cmd()
    if not _check_freqtrade_runnable(ft_base, cwd=repo_root):
        fallback = [sys.executable, "-m", "freqtrade"]
        _log("freqtrade not runnable with resolved command, trying same interpreter: python -m freqtrade ...")
        if _check_freqtrade_runnable(fallback, cwd=repo_root):
            ft_base = fallback
            _log("using: " + " ".join(ft_base))
        else:
            _log("Aborted: freqtrade not runnable (see fix above).")
            return []
    use_docker = any(
        x in " ".join(ft_base).lower() for x in ("docker", "compose")
    ) or bool(os.environ.get("FREQTRADE_USER_DATA_CONTAINER"))
    if use_docker:
        wfa_cwd = repo_root
        ft_base = _docker_cmd_with_project_mount(ft_base, repo_root)
        # Absolute path in container so Freqtrade writes to mounted dir (host user_data/backtest_results)
        userdir_for_cmd = "/app/user_data"
        config_path_in_container = "/app/user_data/config.json"
    else:
        wfa_cwd = user_data
        userdir_for_cmd = "."
        config_path_in_container = None
    initial_balance = get_initial_balance_from_freqtrade_config(user_data)
    out_with_wfa = []
    for idx, item in enumerate(backtest_results):
        _print_wfa_progress(idx + 1, len(backtest_results), 0, 1, "Preparing", "Running")
        cfg = item.get("backtestResult", {}).get("config", {})
        start_str = (cfg.get("startDate") or "").strip()[:10]
        end_str = (cfg.get("endDate") or "").strip()[:10]
        strategy_name = _strategy_name_string(item.get("parameters") or {})
        timeframe = _timeframe_from_item(item)
        if not strategy_name or not start_str or not end_str or not timeframe:
            _print_wfa_progress_clear()
            _log(f"Result {idx+1}: SKIP (missing strategy name, startDate, endDate or timeframe in config)")
            continue
        try:
            _validate_strategy_name(strategy_name)
        except ValueError as e:
            _print_wfa_progress_clear()
            _log(f"Result {idx+1}: SKIP {e}")
            continue
        try:
            start_d = datetime.strptime(start_str[:10], "%Y-%m-%d")
            end_d = datetime.strptime(end_str[:10], "%Y-%m-%d")
        except ValueError as e:
            _print_wfa_progress_clear()
            _log(f"Result {idx+1}: SKIP invalid dates: {e}")
            continue
        available_days = (end_d - start_d).days
        # Minimum days to form one IS+OOS period (need enough data for both segments).
        min_days_for_one_period = 14
        if available_days < min_days_for_one_period:
            _print_wfa_progress_clear()
            _log(f"Result {idx+1}: SKIP date range too short ({available_days}d, need >= {min_days_for_one_period}d)")
            continue
        # Short range: always 1 period with adaptive IS/OOS split (avoid 59//60=0 skipping the result)
        run_one_short_period = available_days < total_window_days
        if run_one_short_period:
            n_periods = 1
        elif use_fixed_periods:
            n_periods = max(1, fixed_periods)
        else:
            n_periods = max(1, available_days // total_window_days)
        periods = []
        performance_transfer_windows = []
        all_oos_trades: list[dict] = []
        window_failed = False
        if run_one_short_period:
            is_ratio = is_size_days / total_window_days if total_window_days else 0.75
            is_days_actual = max(1, int(available_days * is_ratio))
            window_start = start_d
            window_end = end_d
            is_start = window_start
            is_end = window_start + timedelta(days=is_days_actual)
            oos_start = is_end
            oos_end = window_end
            is_start_str = is_start.strftime("%Y-%m-%d")
            is_end_str = is_end.strftime("%Y-%m-%d")
            oos_start_str = oos_start.strftime("%Y-%m-%d")
            oos_end_str = oos_end.strftime("%Y-%m-%d")
        for i in range(n_periods):
            if not run_one_short_period:
                window_start = start_d + timedelta(days=i * total_window_days)
                window_end = window_start + timedelta(days=total_window_days)
                if window_end > end_d:
                    _print_wfa_progress_clear()
                    _log(f"Window {i+1}: SKIP (date range too short)")
                    break
                is_start = window_start
                is_end = window_start + timedelta(days=is_size_days)
                oos_start = is_end
                oos_end = window_end
                is_start_str = is_start.strftime("%Y-%m-%d")
                is_end_str = is_end.strftime("%Y-%m-%d")
                oos_start_str = oos_start.strftime("%Y-%m-%d")
                oos_end_str = oos_end.strftime("%Y-%m-%d")
            else:
                # Defensive: ensure window_start/window_end set even if pre-loop block were removed
                window_start = start_d
                window_end = end_d
                if i > 0:
                    break
            # (is_start_str, is_end_str, oos_start_str, oos_end_str already set above)
            _print_wfa_progress(idx + 1, len(backtest_results), i + 1, n_periods, "In-Sample", "Running")
            raw_is, curve_is, ret_is = _run_backtest_for_timerange(
                user_data, strategy_name, is_start_str, is_end_str, ft_base,
                userdir_for_cmd, wfa_cwd,
                timeframe=timeframe,
                max_points=points_per_curve, initial_balance=initial_balance,
                config_path_in_container=config_path_in_container,
            )
            _print_wfa_progress(idx + 1, len(backtest_results), i + 1, n_periods, "Out-of-Sample", "Running")
            raw_oos, curve_oos, ret_oos = _run_backtest_for_timerange(
                user_data, strategy_name, oos_start_str, oos_end_str, ft_base,
                userdir_for_cmd, wfa_cwd,
                timeframe=timeframe,
                max_points=points_per_curve, initial_balance=initial_balance,
                config_path_in_container=config_path_in_container,
            )
            if raw_is is None or raw_oos is None:
                _print_wfa_progress_clear()
                _log(f"Window {i+1} failed")
                window_failed = True
                break
            if ret_is is None or ret_oos is None:
                _print_wfa_progress_clear()
                _log(f"Window {i+1}: no profit_total in backtest result, skipping period")
                window_failed = True
                break
            period_label = f"Period {i + 1}"
            is_trades_list = raw_is.get("trades") if isinstance(raw_is.get("trades"), list) else []
            oos_trades_list = raw_oos.get("trades") if isinstance(raw_oos.get("trades"), list) else []
            is_trades_count = len(is_trades_list)
            oos_trades_count = len(oos_trades_list)
            chunk, _ = _freqtrade_trades_to_oos_trades(oos_trades_list, initial_balance)
            all_oos_trades.extend(chunk)
            is_profit_factor = _profit_factor_from_raw(raw_is)
            oos_profit_factor = _profit_factor_from_raw(raw_oos)
            validation_max_dd = _validation_max_dd_from_raw(raw_oos)
            if not curve_oos or len(curve_oos) < 2:
                _log(
                    f"   Window {i+1}: WARNING oosEquityCurve empty or single point (curve_len={len(curve_oos) if curve_oos else 0}, oos_trades={oos_trades_count}) - OOS risk metrics (e.g. Max Drawdown) will be n/a on Kiploks. "
                    + ("oos_trades=0: no trades in this OOS window." if oos_trades_count == 0 else "oos_trades>0 but curve empty: check result file read or date format in _trades_to_equity_curve_kiploks.")
                )
            performance_transfer_windows.append({
                "window": period_label,
                "isPeriod": {"start": is_start_str + "T00:00:00.000Z", "end": is_end_str + "T00:00:00.000Z"},
                "oosPeriod": {"start": oos_start_str + "T00:00:00.000Z", "end": oos_end_str + "T00:00:00.000Z"},
                "isEquityCurve": curve_is[:points_per_curve] if curve_is else [],
                "oosEquityCurve": curve_oos[:points_per_curve] if curve_oos else [],
                "isTradesCount": is_trades_count,
                "oosTradesCount": oos_trades_count,
                **({"isProfitFactor": is_profit_factor} if is_profit_factor is not None else {}),
                **({"oosProfitFactor": oos_profit_factor} if oos_profit_factor is not None else {}),
                **({"validationMaxDD": validation_max_dd} if validation_max_dd is not None else {}),
            })
            # Attach parameters so backend can compute PSI (same params for all windows = stable)
            period_params = _numeric_params_only(item.get("parameters") or {})
            if not period_params and raw_is:
                cfg = raw_is.get("config") or {}
                period_params = _numeric_params_only(
                    raw_is.get("params") or raw_is.get("strategy_params") or cfg.get("strategy_params") or {}
                )
            periods.append({
                "startDate": window_start.strftime("%Y-%m-%d"),
                "endDate": window_end.strftime("%Y-%m-%d"),
                "isStart": is_start_str,
                "isEnd": is_end_str,
                "oosStart": oos_start_str,
                "oosEnd": oos_end_str,
                "optimizationReturn": ret_is,
                "validationReturn": ret_oos,
                "isTradesCount": is_trades_count,
                "oosTradesCount": oos_trades_count,
                **({"isProfitFactor": is_profit_factor} if is_profit_factor is not None else {}),
                **({"oosProfitFactor": oos_profit_factor} if oos_profit_factor is not None else {}),
                **({"validationMaxDD": validation_max_dd} if validation_max_dd is not None else {}),
                **({"parameters": period_params} if period_params else {}),
            })
            print(f"  Result {idx + 1}/{len(backtest_results)}  Window {i + 1}/{n_periods} done", file=sys.stderr)
            sys.stderr.flush()

        if window_failed or not periods:
            if window_failed:
                _print_wfa_progress_clear()
                _log("Skipped: not all windows succeeded")
            continue
        # Do not add retention, decay, consistency, wfe, stabilityScore, overfittingScore - backend computes these from periods and performanceTransfer.
        # Do not add cross-window statistics (mean/std/variance across windows, min/max OOS, skewness/kurtosis, correlation IS↔OOS, worst-window penalties) - backend only; see docs/PLAN_CROSS_WINDOW_STATISTICS_BACKEND_ONLY.md.
        wfa_payload = {"periods": periods}
        if performance_transfer_windows:
            wfa_payload["performanceTransfer"] = {
                "windows": performance_transfer_windows,
                "pointsPerCurve": points_per_curve,
            }
        item["walkForwardAnalysis"] = wfa_payload
        item["oos_trades"] = all_oos_trades
        if not all_oos_trades:
            _print_wfa_progress_clear()
            _log(f"Result {idx + 1}: SKIP no OOS trades (backend requires non-empty oos_trades)")
            continue
        # Explicit date range for backend Data Quality Guard (robustness score)
        cfg = item.get("backtestResult", {}).get("config", {})
        if cfg.get("startDate") and cfg.get("endDate"):
            item["dateFrom"] = cfg["startDate"][:10] if len(cfg.get("startDate", "")) >= 10 else cfg["startDate"]
            item["dateTo"] = cfg["endDate"][:10] if len(cfg.get("endDate", "")) >= 10 else cfg["endDate"]
        out_with_wfa.append(item)
        _print_wfa_progress(idx + 1, len(backtest_results), n_periods, n_periods, "Out-of-Sample", "Done")
        _print_wfa_progress_clear()
    if backtest_results and not out_with_wfa:
        _print_wfa_progress_clear()
        _log("All result(s) skipped (see errors above)")
    return out_with_wfa


def _symbol_from_item(item: dict) -> str:
    """Extract symbol from result item (backtestResult.config, results, or strategy). Empty if missing."""
    if not item or not isinstance(item, dict):
        return ""
    bt = item.get("backtestResult") or {}
    config = bt.get("config") or {}
    results = bt.get("results") or {}
    strategy = item.get("strategy") or {}
    raw = (
        results.get("symbol")
        or bt.get("symbol")
        or config.get("symbol")
        or strategy.get("symbol")
        or ""
    )
    if not raw or not isinstance(raw, str):
        return ""
    return str(raw).strip()


def validate_result_for_kiploks(item: dict) -> tuple[bool, str]:
    """
    Validate one result before upload. WFA required - without it we cannot do analysis; reject and stop.
    Returns (ok, error_message).
    """
    if not item or not isinstance(item, dict):
        return False, "Empty or invalid result"
    bt = item.get("backtestResult")
    if not bt or not isinstance(bt, dict):
        return False, "Missing backtestResult"
    if not bt.get("results"):
        return False, "Missing backtestResult.results"
    symbol = _symbol_from_item(item)
    if not symbol:
        return False, "Missing symbol (required for analysis). Do not send."
    params = item.get("parameters") or {}
    strat = params.get("strategy")
    if strat is None or (isinstance(strat, dict) and not strat):
        return False, "Missing parameters.strategy"
    wfa = item.get("walkForwardAnalysis")
    if not wfa or not isinstance(wfa, dict):
        return False, "Missing walkForwardAnalysis"
    oos_trades = item.get("oos_trades")
    if isinstance(oos_trades, list) and len(oos_trades) > 0:
        pass  # trade-level OOS present
    else:
        # Align with Kiploks API: WFA periods/windows (or performanceTransfer.windows) can substitute empty oos_trades.
        ptwin = (wfa.get("performanceTransfer") or {}).get("windows") if isinstance(wfa.get("performanceTransfer"), dict) else None
        if not isinstance(ptwin, list) or len(ptwin) == 0:
            periods_precheck = wfa.get("periods") or wfa.get("windows") or []
            if not isinstance(periods_precheck, list) or len(periods_precheck) == 0:
                return False, "Missing or empty oos_trades and no WFA periods/windows to derive OOS from (server requires one or the other)"
    periods = wfa.get("periods") or wfa.get("windows") or []
    if not periods:
        return False, "Missing walkForwardAnalysis.periods (or .windows)"
    for i, p in enumerate(periods):
        if not isinstance(p, dict):
            return False, f"WFA period[{i}] is not an object"
        opt = p.get("optimizationReturn")
        if opt is None and isinstance(p.get("metrics"), dict):
            opt = (p["metrics"].get("optimization") or {}).get("totalReturn") or (p["metrics"].get("optimization") or {}).get("total")
        val = p.get("validationReturn")
        if val is None and isinstance(p.get("metrics"), dict):
            val = (p["metrics"].get("validation") or {}).get("totalReturn") or (p["metrics"].get("validation") or {}).get("total")
        if opt is None or val is None:
            return False, f"WFA period[{i}] missing optimizationReturn or validationReturn"
        try:
            o, v = float(opt), float(val)
            if o != o or v != v:
                return False, f"WFA period[{i}] optimizationReturn/validationReturn must be finite numbers"
        except (TypeError, ValueError):
            return False, f"WFA period[{i}] non-numeric optimizationReturn/validationReturn"
    return True, ""


def _redact_token(token: str) -> str:
    """Return a safe string for logging; never log any part of the api_token."""
    return "Bearer ***"


def _validate_api_credentials(api_url: str, api_token: str) -> bool:
    """Validate api_url and api_token before upload. Returns False if invalid (empty, short token, or non-HTTPS URL).
    Never logs the token value."""
    api_url = (api_url or "").strip()
    api_token = (api_token or "").strip()
    if not api_url or not api_token:
        return False
    if len(api_token) < 32:
        _log("API token too short (min 32 chars). Skipping upload.")
        return False
    try:
        parsed = urlparse(api_url)
        if not parsed.netloc:
            _log("Invalid API URL (no host). Skipping upload.")
            return False
        if parsed.scheme != "https":
            host = (parsed.hostname or "").lower()
            if host not in ("localhost", "127.0.0.1", "host.docker.internal"):
                _log("API URL must use HTTPS (except localhost). Skipping upload.")
                return False
    except Exception:
        return False
    return True


def _fetch_analyze_status(api_url: str, api_token: str) -> dict | None:
    """GET /api/integration/analyze-status. Returns parsed JSON or None on error.
    Uses a 15s timeout for the request; DNS resolution time may add to total wait."""
    url = f"{api_url.rstrip('/')}/api/integration/analyze-status"
    headers = {"Authorization": f"Bearer {api_token}"}
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            if 200 <= resp.getcode() < 300:
                return json.loads(resp.read().decode())
    except Exception:
        pass
    return None


def _log_analyze_status(
    status: dict,
    deferred_lines: list[str] | None = None,
) -> None:
    """Print one-line status to stderr, or append to deferred_lines for storage full (so it prints above ADVANCED ANALYSIS header)."""
    if status.get("storageFull"):
        used = status.get("storageUsed", "?")
        limit = status.get("storageLimit", 30)
        line = f"   • Storage {used}/{limit} - delete some tests in Kiploks to upload new ones."
        if deferred_lines is not None:
            deferred_lines.append(line)
        else:
            print(line, file=sys.stderr)
            sys.stderr.flush()
        return
    if not status.get("allowed") and status.get("retryAfterSeconds"):
        sec = status["retryAfterSeconds"]
        print(f"   • Next API request allowed in {sec}s.", file=sys.stderr)
        sys.stderr.flush()


def _finite_float(v: float | None) -> float | None:
    """Return v if finite (not NaN, not inf), else None. Caller is responsible for domain validation."""
    if v is None or v != v or abs(v) == float("inf"):
        return None
    return v


def _json_serializable(obj: object, _depth: int = 0) -> object:
    """Recursively convert payload to JSON-serializable types (pandas Timestamp, numpy, etc.). NaN/Inf -> None. Max depth 50 to avoid RecursionError."""
    if _depth > 50:
        return str(obj)
    if hasattr(obj, "isoformat") and callable(getattr(obj, "isoformat", None)):
        try:
            return obj.isoformat()
        except Exception:
            pass
    if hasattr(obj, "item") and callable(getattr(obj, "item", None)):
        try:
            return obj.item()
        except Exception:
            pass
    if isinstance(obj, dict):
        return {str(k): _json_serializable(v, _depth + 1) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_serializable(v, _depth + 1) for v in obj]
    if isinstance(obj, float):
        return obj if _finite_float(obj) is not None else None
    if isinstance(obj, (str, int, bool)) or obj is None:
        return obj
    try:
        import numpy as np
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj) if np.isfinite(obj) else None
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.ndarray):
            return _json_serializable(obj.tolist(), _depth + 1)
    except ImportError:
        pass
    try:
        import pandas as pd
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if hasattr(obj, "tolist"):
            return obj.tolist()
    except ImportError:
        pass
    return obj


def export_test_result_to_json(
    results: list[dict],
    path: Path,
    config: dict,
) -> None:
    """Write the same payload that would be sent to Kiploks API to a JSON file.
    Path must be under script dir (same as uploaded.json) so it persists when run in Docker."""
    payload = {
        "results": _json_serializable(results),
        "source": "freqtrade",
        "exported_at": datetime.now(timezone.utc).isoformat(),
    }
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    _log(f"Exported test result payload to {path} ({len(results)} result(s)).")


def upload_to_kiploks(
    api_url: str,
    results: list[dict],
    config: dict,
    *,
    api_token: str,
) -> tuple[bool, list[str], list[str], list[str]]:
    """POST results to Kiploks integration API. Returns (success, analyze_urls, report_ids, error_lines). Handles 429 (retry once) and 403 storage_limit."""
    base = api_url.rstrip("/")
    url = f"{base}/api/integration/results"
    payload = {
        "results": _json_serializable(results),
        "source": "freqtrade",
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}",
    }
    debug_http = os.getenv("KIPLOKS_DEBUG_HTTP", "").strip().lower() in ("1", "true", "yes", "on")

    def _sanitize_error_text(text: str, *, max_len: int = 1200) -> str:
        """Hide secrets and truncate oversized response text before logging."""
        safe = str(text or "")
        safe = re.sub(r"Bearer\s+[A-Za-z0-9._\-]+", "Bearer ***", safe, flags=re.IGNORECASE)
        safe = re.sub(r"api[_-]?token[=:\s]+[A-Za-z0-9._\-]+", "api_token=***", safe, flags=re.IGNORECASE)
        safe = re.sub(r"api[_-]?key[=:\s]+[A-Za-z0-9._\-]+", "api_key=***", safe, flags=re.IGNORECASE)
        if len(safe) > max_len:
            return safe[:max_len] + "... (truncated)"
        return safe

    def _do_post() -> tuple[int, dict | None, str | None, str | None, str | None, list[str], list[str]]:
        """Returns (status_code, error_body_or_none, error_raw_or_none, server_header_or_none, content_type_or_none, analyze_urls, report_ids)."""
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                if 200 <= resp.getcode() < 300:
                    data = json.loads(resp.read().decode())
                    urls = data.get("analyzeUrls") or []
                    rids = data.get("reportIds") or data.get("resultIds") or []
                    if not isinstance(urls, list):
                        urls = []
                    if not isinstance(rids, list):
                        rids = []
                    return resp.getcode(), None, None, None, None, [str(u) for u in urls], [str(x) for x in rids]
                return resp.getcode(), None, None, resp.headers.get("Server"), resp.headers.get("Content-Type"), [], []
        except urllib.error.HTTPError as e:
            err_body = None
            err_raw = None
            try:
                raw = e.read().decode()
                err_raw = _sanitize_error_text(raw)
                err_body = json.loads(raw) if raw.strip() else None
            except Exception:
                pass
            return e.code, err_body, err_raw, e.headers.get("Server"), e.headers.get("Content-Type"), [], []

    error_lines: list[str] = []
    code, err_body, err_raw, err_server, err_content_type, urls, report_ids = _do_post()
    if 200 <= code < 300:
        return True, urls, report_ids, []

    if code == 429 and err_body:
        retry_sec = err_body.get("retryAfterSeconds")
        if retry_sec is not None and retry_sec > 0:
            print(f"   • Rate limit: next request in {retry_sec}s. Waiting...", file=sys.stderr)
            sys.stderr.flush()
            time.sleep(retry_sec)
            code, err_body, err_raw, err_server, err_content_type, urls, report_ids = _do_post()
            if 200 <= code < 300:
                return True, urls, report_ids, []
            if code == 429:
                print(f"   • Rate limit: still not allowed. Try again later.", file=sys.stderr)
                sys.stderr.flush()
                return False, [], [], []
        else:
            print(f"   • Rate limit: only one analyze request per minute.", file=sys.stderr)
            sys.stderr.flush()
            return False, [], [], []

    if code == 403 and err_body and err_body.get("error") == "storage_limit":
        msg = err_body.get("message", "Storage limit reached.")
        current = err_body.get("currentCount", "?")
        limit = err_body.get("limit", 30)
        requested = err_body.get("requested", "?")
        error_lines.append(f"Upload failed: HTTP {code} (auth: {_redact_token(api_token)})")
        error_lines.append(f"   • {msg}")
        error_lines.append(f"   • Stored: {current}/{limit}. Requested: {requested}. Delete some tests in Kiploks.")
        return False, [], [], error_lines

    # Do not show auth error message when token was never set (user did not configure api_token).
    if code == 401 and not (api_token and str(api_token).strip()):
        return False, [], [], []

    error_lines.append(f"Upload failed: HTTP {code} (auth: {_redact_token(api_token)})")
    error_lines.append(f"   • Upload URL: {url}")
    if debug_http and err_server:
        error_lines.append(f"   • Response Server: {err_server}")
    if debug_http and err_content_type:
        error_lines.append(f"   • Response Content-Type: {err_content_type}")
    if err_body and err_body.get("message"):
        error_lines.append(f"   • {err_body['message']}")
    if debug_http and err_raw:
        error_lines.append(f"   • Raw response: {err_raw}")
    elif debug_http and code >= 400:
        error_lines.append("   • Raw response: <empty body>")
    if not debug_http:
        error_lines.append("   • Detailed HTTP debug is hidden by default for safety. Set KIPLOKS_DEBUG_HTTP=1 locally to inspect sanitized response headers/body.")
    return False, [], [], error_lines


def _stamp_kiploks_analyze_urls_on_results(results: list[dict], analyze_urls: list[str]) -> None:
    """Embed cloud analyze URLs into each result dict (for export_test_result.json and optional local re-POST)."""
    for j, r in enumerate(results):
        if j >= len(analyze_urls):
            break
        u = str(analyze_urls[j]).strip()
        if not u or "/analyze/" not in u:
            continue
        if u.startswith("https://") and "kiploks.com" in u:
            r["kiploksAnalyzeUrl"] = u


def _patch_local_orchestrator_reports_with_urls(
    api_url: str,
    api_token: str,
    report_ids: list[str],
    analyze_urls: list[str],
) -> None:
    """When api_url points at this orchestrator, attach Kiploks or shell URLs to saved reports via PATCH."""
    try:
        host = (urlparse(api_url).hostname or "").lower()
    except Exception:
        return
    local = (
        host in ("localhost", "127.0.0.1", "host.docker.internal")
        or host.startswith("172.17.")
        or host.startswith("172.18.")
    )
    if not local:
        return
    base = api_url.rstrip("/")
    for j, rid in enumerate(report_ids):
        if j >= len(analyze_urls):
            break
        u = str(analyze_urls[j]).strip()
        patch: dict = {}
        if "/analyze/" in u and "kiploks.com" in u:
            patch["kiploksAnalyzeUrl"] = u
        elif "#report=" in u:
            patch["orchestratorShellUrl"] = u
        if not patch:
            continue
        try:
            req = urllib.request.Request(
                f"{base}/api/reports/{rid}",
                data=json.dumps(patch).encode("utf-8"),
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_token}"},
                method="PATCH",
            )
            with urllib.request.urlopen(req, timeout=30):
                pass
        except Exception:
            pass


def main() -> int:
    _print_header()
    print("Loading...", file=sys.stderr)
    sys.stderr.flush()
    user_data = get_user_data_root()
    if not user_data.is_dir():
        print("not found user_data folder (expected", user_data, "). Run from repo root.", file=sys.stderr)
        return 1
    config = load_config()
    if not config:
        print("Config not found or empty (expected kiploks.json in the same folder as run.py). Copy from kiploks.json.example.", file=sys.stderr)
        return 1
    top_n = int(config.get("top_n", 3))
    results_dir = user_data / "backtest_results"
    if not results_dir.is_dir():
        print("user_data/backtest_results not found. Run Freqtrade backtesting first.", file=sys.stderr)
        return 1
    keep_n = config.get("keep_last_n_backtest_files")
    if keep_n is None:
        keep_n = 0  # Default: do not leave script-created files; set to N to keep last N
    if isinstance(keep_n, int) and keep_n >= 0:
        prune_kiploks_backtest_files(user_data, keep_n)
    # Always generate WFA so we have oos_trades (required by backend). No legacy path from wfa_results folder.
    print("Scanning backtest results...", file=sys.stderr)
    sys.stderr.flush()
    preview = scan_backtest_results(user_data, top_n)
    wfa_stop = threading.Event()
    wfa_anim = threading.Thread(target=_run_wfa_dots_animation, args=(wfa_stop,), daemon=True)
    wfa_anim.start()
    try:
        results = generate_wfa_for_top(user_data, top_n, config, prefetched=preview)
    finally:
        wfa_stop.set()
        wfa_anim.join(timeout=1.0)
        print("\r" + " " * 32 + "\r", end="", file=sys.stderr)
        sys.stderr.flush()
    _print_wfa_progress_clear()
    if not results:
        results = preview
        _log("Analysis failed. Using " + str(len(results)) + " result(s) without walk-forward data (validation will require it).")
    if not results:
        print("No backtest results to send.")
        return 0
    param_sens_msg: str | None = None
    # Defer hyperopt error output to end of run so "[OK] Backtest loaded" and summary stay visible.
    hyperopt_deferred_errors: list[str] = []
    # Send raw hyperopt trials for backend to compute parameter sensitivity (no formulas on client). Always attempt.
    path = find_latest_fthypt(user_data, config)
    if not path:
        ft_base = _resolve_freqtrade_cmd()
        use_docker = "docker" in " ".join(ft_base).lower() or "compose" in " ".join(ft_base).lower()
        run_hyperopt = use_docker or _running_inside_container()
        if run_hyperopt:
            _log("   • Status: Running hyperopt...")
            repo_root = get_repo_root()
            if use_docker:
                ft_base = _docker_cmd_with_project_mount(ft_base, repo_root)
                userdir_for_cmd = "/app/user_data"
                config_path_in_container = "/app/user_data/config.json"
            else:
                # Inside container (entrypoint python): no mount; use resolved user_data path.
                userdir_for_cmd = str(user_data.resolve())
                config_path_in_container = str(user_data / "config.json") if (user_data / "config.json").is_file() else None
            first = results[0]
            strategy_name = _strategy_name_string(first.get("parameters") or {})
            try:
                _validate_strategy_name(strategy_name)
            except ValueError as e:
                _log(f"   • Skipped hyperopt: {e}")
                strategy_name = ""
            if strategy_name:
                cfg = (first.get("backtestResult") or {}).get("config") or {}
                date_from = (cfg.get("startDate") or "").strip()[:10]
                date_to = (cfg.get("endDate") or "").strip()[:10]
                timeframe = _timeframe_from_item(first)
                if not date_from or not date_to or not timeframe:
                    _log("   • Skipping hyperopt: missing startDate, endDate or timeframe in config.")
                else:
                    timerange = datetime.strptime(date_from, "%Y-%m-%d").strftime("%Y%m%d") + "-" + datetime.strptime(date_to, "%Y-%m-%d").strftime("%Y%m%d")
                    ok, no_data = _run_hyperopt(
                        user_data, strategy_name, timerange, timeframe,
                        ft_base, repo_root, userdir_for_cmd, config_path_in_container, config,
                        deferred_error_lines=hyperopt_deferred_errors,
                    )
                    if ok:
                        _log("   • Status: Hyperopt finished.")
                    else:
                        hyperopt_deferred_errors.append("   • Status: Hyperopt failed (no data or error). See command above to download data.")
            else:
                pass  # strategy_name invalid, hyperopt already skipped above
        else:
            _log("   • Skipped (run with Docker to auto-run hyperopt).")
    path = find_latest_fthypt(user_data, config)
    if path:
        epochs = load_fthypt_epochs(path)
        if len(epochs) >= 3:
            data_points = build_data_points_from_epochs(epochs)
            if len(data_points) >= 3:
                n_params = len(data_points[0].get("parameters") or {}) if data_points else 0
                capped = data_points[:_MAX_HYPEROPT_TRIALS_UPLOAD]
                if len(data_points) > _MAX_HYPEROPT_TRIALS_UPLOAD:
                    _log(f"   • hyperoptTrials capped to {_MAX_HYPEROPT_TRIALS_UPLOAD} (had {len(data_points)}).")
                param_sens_msg = f"Loaded from {path.name} ({len(capped)} epochs, {n_params} params) - sent as hyperoptTrials for backend"
                for r in results:
                    r["hyperoptTrials"] = capped
            else:
                _log("   • Not enough epochs with numeric params for hyperoptTrials.")
        else:
            _log(f"   • Need at least 3 epochs, got {len(epochs)} in {path.name}.")
    else:
        if hyperopt_deferred_errors:
            hyperopt_deferred_errors.append("   • No .fthypt file (set hyperopt_result_path or run hyperopt to create one).")
        else:
            _log("   • No .fthypt file (set hyperopt_result_path or run hyperopt to create one).")
    # Normalize parameters.strategy to string for all results (flat format may have dict)
    for r in results:
        if r.get("parameters") is not None:
            r["parameters"]["strategy"] = _strategy_name_string(r["parameters"]) or r["parameters"].get("strategy") or ""
    valid_results = []
    for r in results:
        ok, err = validate_result_for_kiploks(r)
        if ok:
            valid_results.append(r)
        else:
            print(f"Validation: {err}", file=sys.stderr)
    if not valid_results:
        print("No results passed validation.", file=sys.stderr)
        return 1
    uploaded_files = [r.get("_source_file") for r in valid_results if r.get("_source_file")]
    for r in valid_results:
        r.pop("_source_file", None)

    # Always write full payload to JSON (same dir as uploaded.json so it persists in Docker).
    script_dir = Path(__file__).resolve().parent
    export_path = script_dir / "export_test_result.json"
    try:
        export_test_result_to_json(valid_results, export_path, config)
    except OSError as e:
        _log(f"Export failed: {e}")

    api_token = (config.get("api_token") or "").strip()
    api_url = (config.get("api_url") or "").strip()
    if api_url:
        parsed = urlparse(api_url)
        if parsed.scheme != "https":
            host = (parsed.hostname or "").lower()
            allow_http_dev = host in ("localhost", "127.0.0.1", "host.docker.internal")
            if not allow_http_dev:
                raise ValueError("api_url must use HTTPS")
    # Non-zero exit if user configured cloud upload but it did not complete (see main() return at end).
    upload_intended = bool(api_url and api_token)
    can_upload = upload_intended
    if can_upload and not _validate_api_credentials(api_url, api_token):
        _log("Invalid API credentials. Skipping upload.")
        can_upload = False
    upload_ok = False
    analyze_urls: list[str] = []
    upload_report_ids: list[str] = []
    deferred_lines: list[str] = []
    if can_upload:
        status = _fetch_analyze_status(api_url, api_token)
        if status:
            _log_analyze_status(status, deferred_lines)
        upload_ok, analyze_urls, upload_report_ids, upload_error_lines = upload_to_kiploks(
            api_url, valid_results, config, api_token=api_token
        )
        deferred_lines.extend(upload_error_lines)
        if upload_ok and analyze_urls:
            _stamp_kiploks_analyze_urls_on_results(valid_results, analyze_urls)
            try:
                export_test_result_to_json(valid_results, export_path, config)
            except OSError as e:
                _log(f"Re-export after upload failed: {e}")
            _patch_local_orchestrator_reports_with_urls(api_url, api_token, upload_report_ids, analyze_urls)
        if upload_ok and uploaded_files:
            _save_uploaded_manifest(uploaded_files)
    if isinstance(keep_n, int) and keep_n == 0:
        prune_kiploks_backtest_files(user_data, 0)
    _print_integration_summary(
        valid_results,
        config,
        can_upload,
        upload_ok,
        analyze_urls=analyze_urls if analyze_urls else None,
        deferred_lines=deferred_lines if deferred_lines else None,
        deferred_hyperopt_errors=hyperopt_deferred_errors if hyperopt_deferred_errors else None,
    )
    sys.stderr.flush()
    if upload_intended and not upload_ok:
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        _cleanup_created_files()
        print("Interrupted.", file=sys.stderr)
        sys.exit(130)
    except (RuntimeError, ValueError) as e:
        _cleanup_created_files()
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        # Do not remove generated files on upload/network errors so user can fix and retry without re-running WFA.
        err_msg = getattr(e, "reason", str(e))
        if hasattr(err_msg, "__str__") and not isinstance(err_msg, str):
            err_msg = str(err_msg)
        err_msg = re.sub(r"Bearer\s+\S+", "Bearer ***", str(err_msg), flags=re.IGNORECASE)
        err_msg = re.sub(r"api[_-]?key=[\w-]+", "api_key=***", err_msg, flags=re.IGNORECASE)
        print(f"Error: {err_msg}", file=sys.stderr)
        sys.exit(1)
