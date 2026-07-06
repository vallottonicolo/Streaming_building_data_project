from datetime import date

from a2b_production.dashboard import RecordStore, build_building_daily_comparison, build_site_daily_comparison


def test_record_store_bounds_prediction_records():
    """Verify raw prediction records still use the configured count bound."""
    store = RecordStore(max_records=2)
    store.upsert("predictions", {"record_key": "a", "event_time": "t1"})
    store.upsert("predictions", {"record_key": "b", "event_time": "t2"})
    store.upsert("predictions", {"record_key": "c", "event_time": "t3"})
    snap = store.snapshot()
    assert [r["record_key"] for r in snap["predictions"]] == ["b", "c"]
    assert snap["received"]["predictions"] == 3


def test_record_store_exposes_filter_metadata_and_unique_counts():
    """Verify dashboard snapshots expose counts and filter option metadata."""
    store = RecordStore(max_records=10)
    store.upsert(
        "building",
        {"record_key": "10|2022-01-01|00-06", "site_id": "2", "building_id": "10", "date": "2022-01-01"},
    )
    store.upsert(
        "building",
        {"record_key": "3|2022-01-01|00-06", "site_id": "1", "building_id": "3", "date": "2022-01-01"},
    )
    store.upsert("site_daily", {"record_key": "12|2022-01-01", "site_id": "12", "date": "2022-01-01"})
    store.upsert("predictions", {"record_key": "p-7", "site_id": "2", "building_id": "7"})

    snap = store.snapshot()

    assert snap["unique_counts"] == {"predictions": 1, "building": 2, "site_daily": 1}
    assert snap["available_sites"] == ["1", "2", "12"]
    assert snap["available_buildings_by_site"] == {"1": ["3"], "2": ["7", "10"]}


def test_building_daily_comparison_uses_preaggregated_daily_predictions():
    """Verify building comparison reads retained daily aggregate rows."""
    records = [
        {"site_id": "1", "building_id": "10", "date": "2022-01-01", "predicted_daily": 4.0},
        {"site_id": "1", "building_id": "10", "date": "2022-01-02", "predicted_daily": 8.0},
    ]

    rows = build_building_daily_comparison(records, {"10|2022-01-01": 7.0})

    assert rows == [
        {
            "site_id": "1",
            "building_id": "10",
            "date": "2022-01-01",
            "predicted_daily": 4.0,
            "real_daily": 7.0,
        }
    ]


def test_building_blocks_are_deduped_before_daily_aggregation():
    """Verify a q7b upsert replaces the old block value instead of double counting."""
    store = RecordStore(max_records=1, retention_days=30)
    store.upsert(
        "building",
        {
            "record_key": "10|2022-01-01|00-06",
            "site_id": "1",
            "building_id": "10",
            "date": "2022-01-01",
            "energy_consumption_6h": 1.5,
        },
    )
    store.upsert(
        "building",
        {
            "record_key": "10|2022-01-01|00-06",
            "site_id": "1",
            "building_id": "10",
            "date": "2022-01-01",
            "energy_consumption_6h": 2.0,
        },
    )
    store.upsert(
        "building",
        {
            "record_key": "10|2022-01-01|06-12",
            "site_id": "1",
            "building_id": "10",
            "date": "2022-01-01",
            "energy_consumption_6h": 3.0,
        },
    )

    rows = store.snapshot()["building"]

    assert rows == [
        {
            "site_id": "1",
            "building_id": "10",
            "date": "2022-01-01",
            "predicted_daily": 5.0,
            "block_count": 2,
        }
    ]


def test_building_cache_keeps_latest_thirty_days_not_raw_record_count():
    """Verify q7b retention is date-window based instead of max raw record based."""
    store = RecordStore(max_records=5, retention_days=30)
    for day in range(1, 32):
        day_str = f"2022-01-{day:02d}"
        for building in range(1, 4):
            store.upsert(
                "building",
                {
                    "record_key": f"{building}|{day_str}|00-06",
                    "site_id": "1",
                    "building_id": str(building),
                    "date": day_str,
                    "energy_consumption_6h": 1.0,
                },
            )

    dates = sorted({row["date"] for row in store.snapshot()["building"]})

    assert dates[0] == "2022-01-02"
    assert dates[-1] == "2022-01-31"
    assert len(dates) == 30
    assert date(2022, 1, 1) not in store.building_block_keys_by_date
    assert date(2022, 1, 1) not in store.building_daily_keys_by_date
    assert sorted(store.building_daily_keys_by_date) == [date(2022, 1, day) for day in range(2, 32)]


def test_building_date_bucket_eviction_drops_whole_old_days():
    """Verify q7b retention evicts by date bucket without losing retained daily totals."""
    store = RecordStore(max_records=5, retention_days=3)
    for day in range(1, 6):
        day_str = f"2022-01-{day:02d}"
        for hour_block in ["00-06", "06-12"]:
            store.upsert(
                "building",
                {
                    "record_key": f"10|{day_str}|{hour_block}",
                    "site_id": "1",
                    "building_id": "10",
                    "date": day_str,
                    "hour_block": hour_block,
                    "energy_consumption_6h": 1.0,
                },
            )

    snap = store.snapshot()

    assert [row["date"] for row in snap["building"]] == ["2022-01-03", "2022-01-04", "2022-01-05"]
    assert [row["predicted_daily"] for row in snap["building"]] == [2.0, 2.0, 2.0]
    assert sorted(store.building_block_keys_by_date) == [date(2022, 1, day) for day in range(3, 6)]
    assert sorted(store.building_daily_keys_by_date) == [date(2022, 1, day) for day in range(3, 6)]


def test_site_daily_cache_uses_same_retention_window():
    """Verify site daily chart rows are evicted by date window."""
    store = RecordStore(max_records=2, retention_days=3)
    for day in range(1, 6):
        day_str = f"2022-01-{day:02d}"
        store.upsert(
            "site_daily",
            {
                "record_key": f"1|{day_str}",
                "site_id": "1",
                "date": day_str,
                "daily_consumption_site": float(day),
            },
        )

    assert [row["date"] for row in store.snapshot()["site_daily"]] == ["2022-01-03", "2022-01-04", "2022-01-05"]
    assert sorted(store.site_daily_keys_by_date) == [date(2022, 1, day) for day in range(3, 6)]


def test_building_lag_does_not_evict_site_daily_window():
    """Verify q7b running ahead does not make earlier q7c site rows disappear."""
    store = RecordStore(max_records=2, retention_days=3)
    for day in range(1, 11):
        day_str = f"2022-01-{day:02d}"
        store.upsert(
            "building",
            {
                "record_key": f"10|{day_str}|00-06",
                "site_id": "1",
                "building_id": "10",
                "date": day_str,
                "energy_consumption_6h": 1.0,
            },
        )
    for day in range(1, 4):
        day_str = f"2022-01-{day:02d}"
        store.upsert(
            "site_daily",
            {
                "record_key": f"1|{day_str}",
                "site_id": "1",
                "date": day_str,
                "daily_consumption_site": float(day),
            },
        )

    snap = store.snapshot()

    assert [row["date"] for row in snap["building"]] == ["2022-01-08", "2022-01-09", "2022-01-10"]
    assert [row["date"] for row in snap["site_daily"]] == ["2022-01-01", "2022-01-02", "2022-01-03"]


def test_site_daily_comparison_uses_exact_site_date_match():
    """Verify site daily comparison only includes exact site/date matches."""
    records = [
        {"site_id": "1", "date": "2022-01-01", "daily_consumption_site": 12.0},
        {"site_id": "1", "date": "2022-01-02", "daily_consumption_site": 14.0},
    ]

    rows = build_site_daily_comparison(records, {"1|2022-01-02": 20.0})

    assert rows == [
        {
            "site_id": "1",
            "date": "2022-01-02",
            "predicted_daily": 14.0,
            "real_daily": 20.0,
        }
    ]
