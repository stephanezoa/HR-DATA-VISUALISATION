from __future__ import annotations

import hashlib
import json
import pickle
import threading
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from generate_arrets_reports import DEFAULT_FOCUS_NATURES, WorkbookDataset

from .config import ANALYSIS_CACHE_DIRNAME, DATASET_SNAPSHOT_META_NAME, DATASET_SNAPSHOT_NAME
from .logging_setup import get_logger
from .storage import resolve_session_workbook_name
from .utils import read_excel_metadata

logger = get_logger("analysis")


DATASET_CACHE: dict[str, tuple[dict[str, Any], WorkbookDataset]] = {}
DATASET_CACHE_LOCK = threading.Lock()
ANALYSIS_CACHE: dict[str, dict[str, Any]] = {}
ANALYSIS_CACHE_LOCK = threading.Lock()
PREWARMED_ANALYSES: set[str] = set()
PREWARMED_ANALYSES_LOCK = threading.Lock()


def workbook_signature(workbook_path: Path) -> dict[str, Any]:
    stat = workbook_path.stat()
    return {
        "path": str(workbook_path.resolve()),
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def session_cache_dir(job_path: Path) -> Path:
    path = job_path / ANALYSIS_CACHE_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def dataset_snapshot_paths(workbook_path: Path) -> tuple[Path, Path]:
    job_path = workbook_path.parent
    return (
        job_path / DATASET_SNAPSHOT_NAME,
        job_path / DATASET_SNAPSHOT_META_NAME,
    )


def cache_key(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def remember_dataset_cache(cache_key_value: str, signature: dict[str, Any], dataset: WorkbookDataset) -> WorkbookDataset:
    with DATASET_CACHE_LOCK:
        DATASET_CACHE[cache_key_value] = (signature, dataset)
        if len(DATASET_CACHE) > 12:
            stale_key = next(iter(DATASET_CACHE))
            if stale_key != cache_key_value:
                DATASET_CACHE.pop(stale_key, None)
    return dataset


def load_dataset_snapshot(workbook_path: Path, signature: dict[str, Any]) -> WorkbookDataset | None:
    snapshot_path, meta_path = dataset_snapshot_paths(workbook_path)
    if not snapshot_path.exists() or not meta_path.exists():
        return None
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        if payload.get("signature") != signature:
            return None
        with snapshot_path.open("rb") as handle:
            dataset = pickle.load(handle)
        if isinstance(dataset, WorkbookDataset):
            return dataset
    except Exception:
        return None
    return None


def store_dataset_snapshot(workbook_path: Path, signature: dict[str, Any], dataset: WorkbookDataset) -> None:
    snapshot_path, meta_path = dataset_snapshot_paths(workbook_path)
    tmp_snapshot = snapshot_path.with_name(f"{snapshot_path.name}.tmp")
    tmp_meta = meta_path.with_name(f"{meta_path.name}.tmp")
    try:
        with tmp_snapshot.open("wb") as handle:
            pickle.dump(dataset, handle, protocol=pickle.HIGHEST_PROTOCOL)
        tmp_meta.write_text(
            json.dumps({"signature": signature}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_snapshot.replace(snapshot_path)
        tmp_meta.replace(meta_path)
    except Exception:
        try:
            tmp_snapshot.unlink(missing_ok=True)
        except Exception:
            pass
        try:
            tmp_meta.unlink(missing_ok=True)
        except Exception:
            pass


def get_workbook_dataset(workbook_path: Path) -> WorkbookDataset:
    signature = workbook_signature(workbook_path)
    current_key = signature["path"]
    with DATASET_CACHE_LOCK:
        cached = DATASET_CACHE.get(current_key)
        if cached and cached[0] == signature:
            logger.info("Dataset servi depuis le cache mémoire: %s", workbook_path.name)
            return cached[1]

    cached_snapshot = load_dataset_snapshot(workbook_path, signature)
    if cached_snapshot is not None:
        logger.info("Dataset rechargé depuis le snapshot disque: %s", workbook_path.name)
        return remember_dataset_cache(current_key, signature, cached_snapshot)

    dataset = WorkbookDataset(workbook_path=workbook_path, focus_natures=DEFAULT_FOCUS_NATURES)
    store_dataset_snapshot(workbook_path, signature, dataset)
    logger.info("Dataset construit depuis Excel et persisté: %s", workbook_path.name)
    return remember_dataset_cache(current_key, signature, dataset)


def get_cached_analysis(
    job_path: Path,
    workbook_path: Path,
    namespace: str,
    params: dict[str, Any],
    builder: Any,
) -> dict[str, Any]:
    key_payload = {
        "namespace": namespace,
        "params": params,
        "signature": workbook_signature(workbook_path),
    }
    cache_path = session_cache_dir(job_path) / f"{namespace}_{cache_key(key_payload)}.json"

    with ANALYSIS_CACHE_LOCK:
        cached = ANALYSIS_CACHE.get(str(cache_path))
        if cached is not None:
            return cached

    if cache_path.exists():
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            if payload.get("signature") == key_payload["signature"]:
                data = payload.get("data") or {}
                with ANALYSIS_CACHE_LOCK:
                    ANALYSIS_CACHE[str(cache_path)] = data
                return data
        except Exception:
            pass

    data = builder()
    cache_path.write_text(
        json.dumps({"signature": key_payload["signature"], "data": data}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    with ANALYSIS_CACHE_LOCK:
        ANALYSIS_CACHE[str(cache_path)] = data
        if len(ANALYSIS_CACHE) > 64:
            stale_key = next(iter(ANALYSIS_CACHE))
            if stale_key != str(cache_path):
                ANALYSIS_CACHE.pop(stale_key, None)
    return data


def get_workbook_metadata(job_path: Path, workbook_path: Path) -> dict[str, Any]:
    return get_cached_analysis(
        job_path=job_path,
        workbook_path=workbook_path,
        namespace="workbook_meta",
        params={"view": "default"},
        builder=lambda: read_excel_metadata(workbook_path),
    )


def build_inspection_payload(dataset: WorkbookDataset) -> dict[str, Any]:
    default_year = int(dataset.available_years[-1])
    groups = dataset.available_groups(
        year=default_year,
        chains=None,
        natures=list(DEFAULT_FOCUS_NATURES),
    )
    return {
        "years": [int(year) for year in dataset.available_years],
        "default_year": default_year,
        "group_rows": [
            {
                "chain": group.chain,
                "nature": group.nature,
                "equipment_count": len(dataset.equipments_for_group(default_year, group)),
            }
            for group in groups
        ],
    }


def build_pareto_payload(
    dataset: WorkbookDataset,
    year: int,
    selected_chain: str = "",
    selected_nature: str = "",
) -> dict[str, Any]:
    equipment_weekly = dataset.equipment_weekly
    year_frame = equipment_weekly[equipment_weekly["annee_da"] == year].copy()
    chain_options = sorted(str(value) for value in year_frame["IDChaine"].dropna().unique().tolist())
    nature_options = sorted(str(value) for value in year_frame["Nature"].dropna().unique().tolist())

    if selected_chain:
        year_frame = year_frame[year_frame["IDChaine"] == selected_chain]
    if selected_nature:
        year_frame = year_frame[year_frame["Nature"] == selected_nature]

    equipment_avg = (
        year_frame.groupby(["IDChaine", "Nature", "Equipement"])["value"]
        .mean()
        .reset_index()
        .sort_values("value", ascending=False)
        .reset_index(drop=True)
    )
    if equipment_avg.empty:
        raise ValueError("Aucune donnée Pareto pour ce filtre.")

    total = float(equipment_avg["value"].sum()) or 1.0
    equipment_avg["cumul_pct"] = (equipment_avg["value"].cumsum() / total * 100).round(1)
    equipment_avg["share_pct"] = (equipment_avg["value"] / total * 100).round(1)
    equipment_avg["value_pct"] = (equipment_avg["value"] * 100).round(2)

    labels = (
        equipment_avg["Equipement"] + " (" + equipment_avg["IDChaine"] + "/" + equipment_avg["Nature"] + ")"
    ).tolist()
    values = [float(value) for value in equipment_avg["value_pct"].tolist()]
    cumul = [float(value) for value in equipment_avg["cumul_pct"].tolist()]
    pareto_cut = int((equipment_avg["cumul_pct"] <= 80).sum()) or min(len(equipment_avg), 1)
    pareto_share = round(float(equipment_avg.head(max(pareto_cut, 1))["share_pct"].sum()), 1)
    peak_equipment = labels[0] if labels else "—"
    peak_value = values[0] if values else 0.0
    median_value = round(float(equipment_avg["value_pct"].median()), 2) if not equipment_avg.empty else 0.0
    rows = [
        {
            "rank": int(index + 1),
            "chain": str(row["IDChaine"]),
            "nature": str(row["Nature"]),
            "equipment": str(row["Equipement"]),
            "value_pct": float(row["value_pct"]),
            "share_pct": float(row["share_pct"]),
            "cumul_pct": float(row["cumul_pct"]),
        }
        for index, row in equipment_avg.iterrows()
    ]
    return {
        "chain_options": chain_options,
        "nature_options": nature_options,
        "labels": labels,
        "values": values,
        "cumul": cumul,
        "pareto_cut": pareto_cut,
        "total_equipment": len(labels),
        "pareto_share": pareto_share,
        "peak_equipment": peak_equipment,
        "peak_value": peak_value,
        "median_value": median_value,
        "rows": rows,
    }


def build_trend_datasets(frame: pd.DataFrame, key_column: str, years: list[int]) -> list[dict[str, Any]]:
    colors = [
        "#0F4C81",
        "#2A9D8F",
        "#C84B31",
        "#6A4C93",
        "#FFB000",
        "#118AB2",
        "#EF476F",
        "#06D6A0",
        "#8338EC",
        "#334155",
    ]
    datasets: list[dict[str, Any]] = []
    for index, key in enumerate(sorted(frame[key_column].dropna().unique().tolist())):
        current = frame[frame[key_column] == key]
        year_map = dict(zip(current["annee_da"].astype(int), current["value_pct"]))
        data_points = [year_map.get(int(year)) for year in years]

        valid_pairs = [(int(year), value) for year, value in zip(years, data_points) if value is not None]
        trend_points = [None] * len(years)
        next_value = None
        slope = None
        if len(valid_pairs) >= 2:
            xs = [pair[0] for pair in valid_pairs]
            ys = [pair[1] for pair in valid_pairs]
            coeffs = np.polyfit(xs, ys, 1)
            slope = round(float(coeffs[0]), 3)
            for j, year in enumerate(years):
                trend_points[j] = round(float(np.polyval(coeffs, int(year))), 2)
            next_value = round(float(np.polyval(coeffs, max(years) + 1)), 2)

        valid_values = [value for value in data_points if value is not None]
        delta = None
        if len(valid_values) >= 2:
            delta = round(float(valid_values[-1] - valid_values[0]), 2)

        datasets.append({
            "key": key,
            "chain": key,
            "nature": key,
            "color": colors[index % len(colors)],
            "data": data_points,
            "trend": trend_points,
            "next": next_value,
            "delta": delta,
            "slope": slope,
            "last": valid_values[-1] if valid_values else None,
        })
    return datasets


def build_trend_payload(dataset: WorkbookDataset) -> dict[str, Any]:
    chain_trend = dataset.chain_weekly.groupby(["annee_da", "IDChaine"])["value"].mean().reset_index()
    nature_trend = dataset.chain_nature_weekly.groupby(["annee_da", "Nature"])["value"].mean().reset_index()
    chain_trend["value_pct"] = (chain_trend["value"] * 100).round(2)
    nature_trend["value_pct"] = (nature_trend["value"] * 100).round(2)

    years = sorted({
        *chain_trend["annee_da"].dropna().astype(int).tolist(),
        *nature_trend["annee_da"].dropna().astype(int).tolist(),
    })
    chain_datasets = build_trend_datasets(chain_trend, "IDChaine", years)
    nature_datasets = build_trend_datasets(nature_trend, "Nature", years)
    chain_summary = [item for item in chain_datasets if item.get("delta") is not None]
    best_chain = min(chain_summary, key=lambda item: item["delta"]) if chain_summary else None
    risk_chain = max(chain_summary, key=lambda item: item["delta"]) if chain_summary else None
    next_year = (max(years) + 1) if len(years) >= 2 else None
    return {
        "years": [int(value) for value in years],
        "next_year": int(next_year) if next_year is not None else None,
        "chain_datasets": chain_datasets,
        "nature_datasets": nature_datasets,
        "best_chain": best_chain,
        "risk_chain": risk_chain,
    }


def build_calendar_payload(dataset: WorkbookDataset) -> dict[str, Any]:
    year = int(dataset.available_years[-1])
    chain_weekly = dataset.chain_weekly
    year_frame = chain_weekly[chain_weekly["annee_da"] == year]
    chains = sorted(str(value) for value in year_frame["IDChaine"].unique().tolist())
    weeks = list(range(1, 53))
    heatmap: dict[str, list[float | None]] = {}
    limits: dict[str, dict[str, float | None]] = {}
    for chain in chains:
        chain_frame = year_frame[year_frame["IDChaine"] == chain]
        week_map = dict(zip(chain_frame["week_num"].astype(int), (chain_frame["value"] * 100).round(2)))
        heatmap[chain] = [
            float(week_map.get(week))
            if week_map.get(week) is not None and pd.notna(week_map.get(week))
            else None
            for week in weeks
        ]
        upper_avg = chain_frame["upper"].mean()
        lower_avg = chain_frame["lower"].mean()
        limits[chain] = {
            "upper": round(float(upper_avg) * 100, 2) if pd.notna(upper_avg) else None,
            "lower": round(float(lower_avg) * 100, 2) if pd.notna(lower_avg) else None,
        }
    return {
        "year": year,
        "years": [int(value) for value in dataset.available_years],
        "chains": chains,
        "weeks": weeks,
        "heatmap": heatmap,
        "limits": limits,
    }


def start_analysis_prewarm(job_id: str, workbook_path: Path) -> None:
    job_path = workbook_path.parent
    try:
        signature = workbook_signature(workbook_path)
    except OSError:
        return
    token = f"{job_id}:{signature['size']}:{signature['mtime_ns']}"
    with PREWARMED_ANALYSES_LOCK:
        if token in PREWARMED_ANALYSES:
            return
        PREWARMED_ANALYSES.add(token)
        if len(PREWARMED_ANALYSES) > 64:
            stale_token = next(iter(PREWARMED_ANALYSES))
            if stale_token != token:
                PREWARMED_ANALYSES.discard(stale_token)

    def runner() -> None:
        try:
            logger.info("Préchauffage des vues d'analyse pour %s", job_id)
            dataset = get_workbook_dataset(workbook_path)
            actual_name = resolve_session_workbook_name(job_path) or workbook_path.name
            default_year = int(dataset.available_years[-1])
            get_workbook_metadata(job_path, workbook_path)
            get_cached_analysis(
                job_path=job_path,
                workbook_path=workbook_path,
                namespace="inspect",
                params={"filename": actual_name},
                builder=lambda: build_inspection_payload(dataset),
            )
            get_cached_analysis(
                job_path=job_path,
                workbook_path=workbook_path,
                namespace="pareto",
                params={"year": default_year, "chain": "", "nature": ""},
                builder=lambda: build_pareto_payload(dataset, default_year),
            )
            get_cached_analysis(
                job_path=job_path,
                workbook_path=workbook_path,
                namespace="trend",
                params={"view": "default"},
                builder=lambda: build_trend_payload(dataset),
            )
            get_cached_analysis(
                job_path=job_path,
                workbook_path=workbook_path,
                namespace="calendar",
                params={"view": "default"},
                builder=lambda: build_calendar_payload(dataset),
            )
            logger.info("Préchauffage terminé pour %s", job_id)
        except Exception:
            logger.exception("Échec du préchauffage pour %s", job_id)
            return

    threading.Thread(target=runner, daemon=True).start()
