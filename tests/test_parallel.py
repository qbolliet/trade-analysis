"""Tests du module de parallélisme intra-pod."""
from __future__ import annotations

import os
import pickle

import pandas as pd
import pytest

from kedro_pipeline import parallel
from kedro_pipeline.parallel import (
    ContextFailure,
    parallel_map,
    resolve_n_jobs,
    serialisable_exception,
)
from macroforecast.tracking import CapturingTracker, RecordingTracker


# ──────────────────────────────────────────────────────────────────────
# resolve_n_jobs
# ──────────────────────────────────────────────────────────────────────

def test_configured_value_wins(monkeypatch):
    monkeypatch.setenv("NUM_CPU", "8")
    assert resolve_n_jobs(3) == 3


@pytest.mark.parametrize("configured", [None, 0, -2])
def test_num_cpu_used_when_not_configured(monkeypatch, configured):
    monkeypatch.setenv("NUM_CPU", "6")
    assert resolve_n_jobs(configured) == 6


@pytest.mark.parametrize("raw", ["", "abc", "0", "-3"])
def test_invalid_num_cpu_falls_back_to_cpu_count(monkeypatch, raw):
    monkeypatch.setenv("NUM_CPU", raw)
    monkeypatch.setattr(parallel.os, "cpu_count", lambda: 4)
    assert resolve_n_jobs(None) == 4


def test_unset_num_cpu_and_unknown_cpu_count(monkeypatch):
    monkeypatch.delenv("NUM_CPU", raising=False)
    monkeypatch.setattr(parallel.os, "cpu_count", lambda: None)
    assert resolve_n_jobs(None) == 1


def test_fractional_num_cpu_is_truncated(monkeypatch):
    monkeypatch.setenv("NUM_CPU", "2.5")
    assert resolve_n_jobs(None) == 2


# ──────────────────────────────────────────────────────────────────────
# parallel_map
# ──────────────────────────────────────────────────────────────────────

def _square(x: int) -> int:
    return x * x


def _env_and_threads(_: int) -> tuple:
    return os.environ.get("OMP_NUM_THREADS"), os.environ.get("XLA_PYTHON_CLIENT_PREALLOCATE")


def _fail_on_three(x: int):
    try:
        if x == 3:
            raise ValueError("boom")
        return x
    except Exception as exc:
        return serialisable_exception(exc)


@pytest.mark.parametrize("n_jobs", [1, 2])
def test_parallel_map_returns_every_item_with_its_index(n_jobs):
    results = dict(parallel_map(_square, range(6), n_jobs))
    assert results == {i: i * i for i in range(6)}


def test_workers_get_single_thread_environment():
    values = {value for _, value in parallel_map(_env_and_threads, range(3), 2)}
    assert values == {("1", "false")}


def test_in_process_run_does_not_touch_parent_environment(monkeypatch):
    monkeypatch.delenv("XLA_PYTHON_CLIENT_PREALLOCATE", raising=False)
    list(parallel_map(_square, [1], 1))
    assert "XLA_PYTHON_CLIENT_PREALLOCATE" not in os.environ


def test_error_returned_by_a_task_does_not_stop_the_others():
    results = dict(parallel_map(_fail_on_three, range(6), 2))
    assert isinstance(results[3], ValueError)
    assert [results[i] for i in (0, 1, 2, 4, 5)] == [0, 1, 2, 4, 5]


def test_threading_backend_is_refused():
    with pytest.raises(ValueError):
        list(parallel_map(_square, [1], 2, backend="threading"))


# ──────────────────────────────────────────────────────────────────────
# Exceptions sérialisables
# ──────────────────────────────────────────────────────────────────────

class _Unpicklable(Exception):
    def __init__(self, a, b):
        super().__init__(a)  # reconstruction impossible avec un seul argument


def test_picklable_exception_is_returned_as_is():
    exc = ValueError("x")
    assert serialisable_exception(exc) is exc


def test_unpicklable_exception_becomes_context_failure():
    try:
        raise _Unpicklable("détail", 2)
    except _Unpicklable as exc:
        wrapped = serialisable_exception(exc)
    assert isinstance(wrapped, ContextFailure)
    assert wrapped.original_type == "_Unpicklable"
    assert "détail" in str(wrapped) and "_Unpicklable" in wrapped.worker_traceback
    restored = pickle.loads(pickle.dumps(wrapped))
    assert str(restored) == str(wrapped) and restored.original_type == "_Unpicklable"


# ──────────────────────────────────────────────────────────────────────
# RecordingTracker
# ──────────────────────────────────────────────────────────────────────

def test_recording_is_replayed_in_order_after_pickling():
    recorder = RecordingTracker()
    recorder.log_metrics({"a": 1.0})
    recorder.set_tags({"k": "v"})
    recorder.log_table(pd.DataFrame({"x": [1]}), "t.csv")
    restored = pickle.loads(pickle.dumps(recorder))
    target = CapturingTracker()
    restored.replay(target)
    assert target.metrics == {"a": 1.0} and target.tags == {"k": "v"}
    assert list(target.tables) == ["t.csv"]


def test_replay_can_keep_only_the_last_table_per_path():
    recorder = RecordingTracker()
    recorder.log_table(pd.DataFrame({"x": [1]}), "t.csv")
    recorder.log_table(pd.DataFrame({"x": [2]}), "t.csv")
    calls = []

    class Spy:
        def log_table(self, df, path):
            calls.append(int(df["x"].iloc[0]))

    recorder.replay(Spy(), only_last_per_artifact=True)
    assert calls == [2]
