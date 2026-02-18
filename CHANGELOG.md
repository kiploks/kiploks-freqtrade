# Changelog

## [1.0.0] - First release

First version of the Kiploks Freqtrade integration.

**Done:**
- Scan backtest results and Walk-Forward Analysis (WFA) generation (required for analysis).
- Upload to Kiploks API; short analyze links in response.
- New terminal UI: "Kiploks Integration Bridge" summary (backtest metrics, equity curve, WFA windows, Data Quality raw facts, advanced metrics hint). Loading and progress messages; no API key required to run - summary and raw data shown anyway.
- Security: FREQTRADE_CMD allowlist (no shell metacharacters); path traversal protection for glob results; ZIP member name sanitization; API token redaction in errors; os.replace fallback on Windows; hyperoptTrials payload cap.
- Unit tests for `_parse_freqtrade_cmd`, `_path_under`, `_validate_strategy_name`, `validate_result_for_kiploks`, legacy path and payload shape.

All notable changes to this project will be documented in this file.
