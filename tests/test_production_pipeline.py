from pathlib import Path

from src.production_pipeline import (
    ingest_and_validate,
    repair_and_align,
    summarize_daily,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT = PROJECT_ROOT / "data" / "historical_production_capstone.csv"


def test_ingestion_rejects_invalid_timestamp():
    valid_records, rejected_records = ingest_and_validate(INPUT)

    assert len(valid_records) == 46
    assert len(rejected_records) == 1

def test_repair_removes_duplicates_and_inserts_slots():
    valid_records, _ = ingest_and_validate(INPUT)
    aligned_records, quality_report = repair_and_align(valid_records)

    quality_counts = dict(
        zip(quality_report["metric"], quality_report["count"])
    )

    assert len(aligned_records) == 48
    assert quality_counts["duplicate_rows_removed"] == 2
    assert quality_counts["six_hour_slots_inserted"] == 4

def test_daily_summary_has_four_slots_per_well_day():
    valid_records, _ = ingest_and_validate(INPUT)
    aligned_records, _ = repair_and_align(valid_records)
    daily_summary = summarize_daily(aligned_records)

    assert len(daily_summary) == 12
    assert (daily_summary["expected_slots"] == 4).all()

def test_missing_readings_stay_missing_and_zero_is_preserved():
    valid_records, _ = ingest_and_validate(INPUT)
    aligned_records, _ = repair_and_align(valid_records)

    assert aligned_records["oil_bbl"].isna().sum() == 6
    assert aligned_records["water_bbl"].isna().sum() == 5
    assert aligned_records["gas_mscf"].isna().sum() == 5
    assert (aligned_records["oil_bbl"] == 0).any()
