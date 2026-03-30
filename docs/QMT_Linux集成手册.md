# QMT Linux Integration Guide (Agent Context)

> **IMPORTANT**: This document is intended for AI coding assistants to understand the current QMT integration status on Linux. **Do NOT attempt to install or import `xtquant` directly on Linux.**

## 1. Architecture: Remote Proxy (RPC)
Since QMT (xtquant) is Windows-only, this codebase uses a **HTTP RPC (Remote Procedure Call)** bridge to communicate between the Linux environment and a Windows machine running QMT.

- **Linux (Client)**: Acts as a proxy. It sends requests via HTTP to the Windows server.
- **Windows (Server)**: Runs `data_engine_qmt/server_windows.py`. It executes the actual `xtquant` commands and returns the results (as JSON or Pickle) to Linux.

## 2. Key Modules & Files
- **`src/zvt/broker/qmt/qmt_remote.py`**: Contains the `QMTForwardClient` and `RemoteObject` proxy logic. It enables calling `xtdata` and `XtQuantTrader` remotely as if they were local.
- **`src/zvt/broker/qmt/qmt_quote.py`**: The "remotified" quote engine. It automatically uses `qmt_remote.xtdata` to fetch code lists, K-line data, and snapshots.
- **`src/zvt/broker/qmt/qmt_account.py`**: The "remotified" trading engine. It uses `QMTForwardClient` to proxy `XtQuantTrader` for orders and position queries.
- **`scripts/update_qmt_data.py`**: The primary entry point for daily data synchronization (Meta & K-line).

## 3. Data Acquisition Modes
### A. Active Polling (HTTP RPC) - **Currently Used**
- **How it works**: Linux sends a POST request -> Windows executes -> Result returns.
- **Supported**: Meta data, Historical K-lines, Snapshots (`get_full_tick`), Order placement, Position queries.
- **Limitation**: Not suitable for high-frequency low-latency updates (e.g., streaming ticks).

### B. Passive Push (Redis Pub/Sub) - **Optional/Experimental**
- **How it works**: Windows publishes to Redis -> Linux subscribes.
- **Use case**: Real-time tick streaming and instant order callbacks.
- **Requirement**: Requires a running Redis server (usually on Linux).

## 4. Environment & Usage
- **Conda Env**: Always use the `quant` environment.
- **Running**: 
  ```bash
  conda run -n quant python scripts/update_qmt_data.py
  ```
- **Configuration**: The Windows server IP is defined in `src/zvt/broker/qmt/qmt_remote.py` (Default: `http://192.168.48.207:8000`).

## 5. Standard Troubleshooting for Agents
- **ModuleNotFoundError: 'xtquant'**: 
  - Cause: Someone tried to import `xtquant` directly on Linux. 
  - Fix: Use `from zvt.broker.qmt.qmt_remote import xtdata` instead.
- **ConnectionError**: 
  - Cause: Windows server is not running or IP/Port is incorrect.
  - Fix: Check if `server_windows.py` is active on the remote machine and verify `base_url` in `qmt_remote.py`.
- **Serialization Error**: 
  - Note: Complex objects (like DataFrames) are handled via Pickle over HTTP. Ensure `qmt_remote.py` logic remains intact.

---
*Last updated: 2026-03-07 by Antigravity*
