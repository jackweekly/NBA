from __future__ import annotations

import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if "pandas" not in sys.modules:
    try:  # pragma: no cover - optional dependency in tests
        import pandas as _pandas  # type: ignore # noqa: F401
    except Exception:  # pragma: no cover - fallback stub
        sys.modules.pop("pandas", None)

        pandas_stub = types.ModuleType("pandas")

        class _StubDataFrame:  # pragma: no cover - lightweight stand-in
            def __init__(self, data=None):
                self._data = list(data or [])
                self.empty = not bool(self._data)

            def insert(self, index: int, column: str, value):
                for row in self._data:
                    row[column] = value

            def to_csv(self, path, index=False):  # noqa: ARG002 - signature parity
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write("stub\n")

            def __len__(self) -> int:
                return len(self._data)

        def _stub_dataframe(data=None):
            if data is None:
                return _StubDataFrame()
            rows = []
            if isinstance(data, dict):
                keys = list(data.keys())
                length = len(next(iter(data.values()), []))
                for idx in range(length):
                    rows.append({key: data[key][idx] for key in keys})
            else:
                rows = list(data)
            return _StubDataFrame(rows)

        def _stub_concat(frames, ignore_index=False):  # noqa: ARG002 - parity
            merged = []
            for frame in frames:
                merged.extend(frame._data)
            return _StubDataFrame(merged)

        pandas_stub.DataFrame = _stub_dataframe  # type: ignore[attr-defined]
        pandas_stub.concat = _stub_concat  # type: ignore[attr-defined]
        sys.modules["pandas"] = pandas_stub

if "requests" not in sys.modules:
    requests_stub = types.ModuleType("requests")

    class _DummySession:
        def get(self, *args, **kwargs):  # pragma: no cover - defensive stub
            raise RuntimeError("requests is not available in the test environment")

    requests_stub.Session = _DummySession  # type: ignore[attr-defined]
    sys.modules["requests"] = requests_stub

if "dateutil" not in sys.modules:
    dateutil_stub = types.ModuleType("dateutil")
    parser_module = types.ModuleType("parser")

    def _parse(value):  # pragma: no cover - defensive stub
        from datetime import datetime

        return datetime.fromisoformat(value)

    parser_module.parse = _parse  # type: ignore[attr-defined]
    dateutil_stub.parser = parser_module  # type: ignore[attr-defined]
    sys.modules["dateutil"] = dateutil_stub
    sys.modules["dateutil.parser"] = parser_module

if "nba_api" not in sys.modules:
    nba_api_stub = types.ModuleType("nba_api")
    stats_module = types.ModuleType("stats")
    endpoints_module = types.ModuleType("endpoints")
    leaguegamelog_module = types.ModuleType("leaguegamelog")

    class _LeagueGameLog:  # pragma: no cover - defensive stub
        def __init__(self, *args, **kwargs):
            raise RuntimeError("nba_api is not available in the test environment")

    endpoints_module.LeagueGameLog = _LeagueGameLog  # type: ignore[attr-defined]
    leaguegamelog_module.LeagueGameLog = _LeagueGameLog  # type: ignore[attr-defined]
    endpoints_module.leaguegamelog = leaguegamelog_module  # type: ignore[attr-defined]
    stats_module.endpoints = endpoints_module  # type: ignore[attr-defined]
    nba_api_stub.stats = stats_module  # type: ignore[attr-defined]

    sys.modules["nba_api"] = nba_api_stub
    sys.modules["nba_api.stats"] = stats_module
    sys.modules["nba_api.stats.endpoints"] = endpoints_module
    sys.modules["nba_api.stats.endpoints.leaguegamelog"] = leaguegamelog_module
