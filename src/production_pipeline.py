from pathlib import Path

import numpy as np
import pandas as pd

REQUIRED_COLUMNS = [
    "well_id",
    "timestamp",
    "oil_bbl",
    "water_bbl",
    "gas_mscf",
    "pressure_psi",
    "status",
    "ingested_at",
]
def ingest_and_validate(
    input_path: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = pd.read_csv(input_path)

    missing = [column for column in REQUIRED_COLUMNS if column not in raw.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    raw["_source_line"] = np.arange(2, len(raw) + 2)
    time_format = "%Y-%m-%d %H:%M"

    raw["timestamp"] = pd.to_datetime(
        raw["timestamp"], format=time_format, errors="coerce"
    )
    raw["ingested_at"] = pd.to_datetime(
        raw["ingested_at"], format=time_format, errors="coerce"
    )

    invalid_timestamp = raw["timestamp"].isna()
    rejected = raw.loc[invalid_timestamp].copy()
    rejected = rejected.rename(columns={"_source_line": "source_line"})
    rejected["rejection_reason"] = "Invalid timestamp"

    valid = raw.loc[~invalid_timestamp].drop(columns="_source_line").copy()
    return valid, rejected
def deduplicate_records(
    records: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    ordered = records.sort_values(
        "ingested_at",
        kind="stable",
        na_position="first",
    )

    before_count = len(ordered)
    deduplicated = ordered.drop_duplicates(
        subset=["well_id", "timestamp"],
        keep="last",
    )
    removed_count = before_count - len(deduplicated)

    deduplicated = deduplicated.sort_values(
        ["well_id", "timestamp"]
    ).reset_index(drop=True)

    return deduplicated, removed_count

def clean_measurements(records: pd.DataFrame) -> pd.DataFrame:
    cleaned = records.copy()

    measurement_columns = [
        "oil_bbl",
        "water_bbl",
        "gas_mscf",
        "pressure_psi",
    ]

    for column in measurement_columns:
        values = pd.to_numeric(cleaned[column], errors="coerce")
        cleaned[column] = values.mask(values < 0)

    return cleaned

def align_six_hourly_grid(records: pd.DataFrame) -> pd.DataFrame:
    aligned_wells = []

    for well_id, well_records in records.groupby("well_id", sort=True):
        expected_times = pd.date_range(
            start=well_records["timestamp"].min(),
            end=well_records["timestamp"].max(),
            freq="6h",
        )

        grid = pd.DataFrame({
            "well_id": well_id,
            "timestamp": expected_times,
        })

        aligned = grid.merge(
            well_records,
            on=["well_id", "timestamp"],
            how="left",
            indicator=True,
            validate="one_to_one",
        )
        aligned["slot_inserted"] = aligned["_merge"] == "left_only"
        aligned = aligned.drop(columns="_merge")
        aligned_wells.append(aligned)

    return pd.concat(aligned_wells, ignore_index=True)


def fill_single_pressure_gaps(records: pd.DataFrame) -> pd.DataFrame:
    repaired = records.copy()

    for _, row_indexes in repaired.groupby("well_id").groups.items():
        row_indexes = list(row_indexes)
        pressures = repaired.loc[row_indexes, "pressure_psi"].tolist()

        for position in range(1, len(pressures) - 1):
            if (
                pd.isna(pressures[position])
                and not pd.isna(pressures[position - 1])
                and not pd.isna(pressures[position + 1])
            ):
                repaired.loc[row_indexes[position], "pressure_psi"] = (
                    pressures[position - 1] + pressures[position + 1]
                ) / 2

    return repaired

def add_derived_measurements(records: pd.DataFrame) -> pd.DataFrame:
    result = records.copy()

    result["liquid_bbl"] = result["oil_bbl"] + result["water_bbl"]
    result["liquid_bbl"] = result["liquid_bbl"].where(
        result["oil_bbl"].notna() & result["water_bbl"].notna()
    )

    result["water_cut_pct"] = (
        result["water_bbl"] / result["liquid_bbl"] * 100
    ).where(result["liquid_bbl"] > 0)

    return result

def repair_and_align(records: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    deduplicated, duplicate_count = deduplicate_records(records)
    cleaned = clean_measurements(deduplicated)
    aligned = align_six_hourly_grid(cleaned)
    aligned = fill_single_pressure_gaps(aligned)
    aligned = add_derived_measurements(aligned)

    measurement_columns = [
        "oil_bbl",
        "water_bbl",
        "gas_mscf",
        "pressure_psi",
    ]

    quality_report = pd.DataFrame([
        {"metric": "duplicate_rows_removed", "count": duplicate_count},
        {
            "metric": "six_hour_slots_inserted",
            "count": int(aligned["slot_inserted"].sum()),
        },
        *[
            {
                "metric": f"missing_{column}",
                "count": int(aligned[column].isna().sum()),
            }
            for column in measurement_columns
        ],
    ])

    return aligned, quality_report

def summarize_daily(records: pd.DataFrame) -> pd.DataFrame:
    daily_records = records.copy()
    daily_records["date"] = daily_records["timestamp"].dt.floor("D")

    summaries = []

    for (well_id, date), group in daily_records.groupby(
        ["well_id", "date"], sort=True
    ):
        expected_slots = len(group)

        summary = {
            "well_id": well_id,
            "date": date,
            "expected_slots": expected_slots,
            "oil_bbl": group["oil_bbl"].sum(min_count=1),
            "water_bbl": group["water_bbl"].sum(min_count=1),
            "gas_mscf": group["gas_mscf"].sum(min_count=1),
        }

        for column in ["oil_bbl", "water_bbl", "gas_mscf"]:
            valid_count = int(group[column].notna().sum())
            summary[f"{column}_valid_readings"] = valid_count
            summary[f"{column}_complete"] = valid_count == expected_slots

        summaries.append(summary)

    return pd.DataFrame(summaries)

def run_pipeline(input_path: Path, output_folder: Path) -> dict[str, pd.DataFrame]:
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    valid_records, rejected_records = ingest_and_validate(input_path)
    cleaned_records, quality_report = repair_and_align(valid_records)
    daily_summary = summarize_daily(cleaned_records)

    invalid_timestamp_row = pd.DataFrame([
        {
            "metric": "invalid_timestamp_rows",
            "count": len(rejected_records),
        }
    ])
    quality_report = pd.concat(
        [invalid_timestamp_row, quality_report],
        ignore_index=True,
    )

    cleaned_records.to_csv(
        output_folder / "cleaned_production.csv", index=False
    )
    quality_report.to_csv(
        output_folder / "data_quality_report.csv", index=False
    )
    daily_summary.to_csv(
        output_folder / "daily_production_summary.csv", index=False
    )
    rejected_records.to_csv(
        output_folder / "rejected_source_records.csv", index=False
    )

    return {
        "cleaned": cleaned_records,
        "quality_report": quality_report,
        "daily_summary": daily_summary,
        "rejected": rejected_records,
    }