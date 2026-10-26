"""Tests for the DataFrame to tuple conversion and the paged bulk insert."""
import numpy as np
import pandas as pd

from etl.load import postgres_loader


def test_df_to_tuples_converts_numpy_and_missing_values():
    df = pd.DataFrame({
        "year": pd.array([2019, None], dtype="Int64"),
        "speed": [np.float64(41.5), np.nan],
        "flag": [np.bool_(True), np.bool_(False)],
        "name": ["a", None],
    })
    rows = postgres_loader._df_to_tuples(df, ["year", "speed", "flag", "name"])
    assert rows[0] == (2019, 41.5, True, "a")
    assert rows[1] == (None, None, False, None)
    assert type(rows[0][0]) is int
    assert type(rows[0][1]) is float


def test_bulk_execute_sums_row_counts_across_pages(monkeypatch):
    class FakeCursor:
        rowcount = 0

    pages = []

    def fake_execute_values(cur, sql, rows, page_size):
        pages.append(len(rows))
        cur.rowcount = len(rows)

    monkeypatch.setattr(postgres_loader.psycopg2.extras, "execute_values", fake_execute_values)
    monkeypatch.setattr(postgres_loader, "PAGE_SIZE", 10)

    total = postgres_loader._bulk_execute(FakeCursor(), "INSERT ...", [(i,) for i in range(25)])

    assert pages == [10, 10, 5]
    assert total == 25
