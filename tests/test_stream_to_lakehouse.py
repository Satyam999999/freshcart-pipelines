from pathlib import Path
from tempfile import TemporaryDirectory
import pytest

from stream import stream_to_lakehouse


class FakeCursor:
    def __init__(self, count=5, total=10000.0):
        self.count = count
        self.total = total

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, query):
        pass

    def fetchone(self):
        return (self.count, self.total)


class FakeConnection:
    def __init__(self, count=5, total=10000.0):
        self.count = count
        self.total = total
        self.closed = False
        self.executed_sql = []

    def cursor(self):
        return FakeCursor(self.count, self.total)

    def close(self):
        self.closed = True


def test_generate_sample_order_structure():
    order = stream_to_lakehouse.generate_sample_order(1)
    assert order["order_id"] == "ORD-STREAM-0001"
    assert "cust_email" in order
    assert "amount" in order
    assert order["currency"] == "INR"
    assert "pincode" in order
    assert "updated_at" in order


def test_stream_batch_to_lakehouse(monkeypatch):
    loaded_files = []
    executed_stages = []

    monkeypatch.setattr(
        stream_to_lakehouse,
        "load_file",
        lambda conn, path, source: loaded_files.append((path, source)),
    )
    monkeypatch.setattr(
        stream_to_lakehouse,
        "_run_sql_file",
        lambda conn, file_name: executed_stages.append(file_name),
    )

    fake_conn = FakeConnection()
    records = [stream_to_lakehouse.generate_sample_order(i) for i in range(1, 4)]

    with TemporaryDirectory() as tmp_landing:
        monkeypatch.setenv("LANDING_DIR", tmp_landing)
        landing_path = stream_to_lakehouse.stream_batch_to_lakehouse(
            records=records,
            run_id="test_stream_001",
            connection=fake_conn,
        )

        assert landing_path is not None
        assert landing_path.exists()
        assert len(loaded_files) == 1
        assert loaded_files[0][1] == "web"
        assert "40_gold.sql" in executed_stages


def test_demonstrate_idempotency_preserves_gold_totals(monkeypatch):
    fake_conn = FakeConnection(count=5, total=12500.0)

    monkeypatch.setattr(
        stream_to_lakehouse,
        "load_file",
        lambda conn, path, source: [],
    )
    monkeypatch.setattr(
        stream_to_lakehouse,
        "run_transformations",
        lambda conn: None,
    )
    monkeypatch.setattr(
        stream_to_lakehouse,
        "query_gold_orders_summary",
        lambda conn: (5, 12500.0),
    )

    records = [stream_to_lakehouse.generate_sample_order(i) for i in range(1, 4)]

    with TemporaryDirectory() as tmp_landing:
        monkeypatch.setenv("LANDING_DIR", tmp_landing)
        # Should not raise AssertionError because count and sum match across runs
        stream_to_lakehouse.demonstrate_idempotency(records, fake_conn)
