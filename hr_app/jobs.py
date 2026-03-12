from __future__ import annotations

import json
import shutil
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from generate_arrets_reports import DEFAULT_FOCUS_NATURES, generate_reports, slugify

from .analysis import get_workbook_dataset
from .config import ARCHIVE_DIR, JOB_DIR, JOB_STATE_NAME
from .logging_setup import get_logger
from .storage import archive_report, resolve_session_workbook_name
from .utils import (
    build_download_bundle,
    read_excel_metadata,
    thematic_bundle_name,
    thematic_combined_pdf_name,
    thematic_group_pdf_name,
)

logger = get_logger("jobs")


@dataclass
class Job:
    id: str
    session_id: str = ""
    title: str = ""
    subtitle: str = ""
    status: str = "pending"
    percent: int = 0
    step: str = "En attente…"
    steps_log: list[str] = field(default_factory=list)
    error: str = ""
    download_path: Path | None = None
    download_name: str = ""
    archive_id: str = ""
    state_path: Path | None = None
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))


@dataclass(frozen=True)
class ExportTask:
    slug: str
    label: str
    groups: list[Any]


JOBS: dict[str, Job] = {}
JOBS_LOCK = threading.Lock()
GEN_LOCK = threading.Semaphore(1)


def job_payload(job: Job) -> dict[str, Any]:
    return {
        "id": job.id,
        "session_id": job.session_id,
        "title": job.title,
        "subtitle": job.subtitle,
        "status": job.status,
        "percent": job.percent,
        "step": job.step,
        "steps_log": list(job.steps_log),
        "error": job.error,
        "download_name": job.download_name,
        "download_path": str(job.download_path) if job.download_path else "",
        "archive_id": job.archive_id,
        "updated_at": job.updated_at,
    }


def persist_job(job: Job) -> None:
    if job.state_path is None:
        return
    job.state_path.parent.mkdir(parents=True, exist_ok=True)
    job.state_path.write_text(json.dumps(job_payload(job), indent=2, ensure_ascii=False), encoding="utf-8")


def job_state_path(job_id: str) -> Path | None:
    matches = list(JOB_DIR.glob(f"*/runs/{job_id}/{JOB_STATE_NAME}"))
    return matches[0] if matches else None


def load_job_from_disk(job_id: str) -> Job | None:
    state_path = job_state_path(job_id)
    if state_path is None or not state_path.exists():
        return None
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    job = Job(
        id=str(data.get("id") or job_id),
        session_id=str(data.get("session_id") or ""),
        title=str(data.get("title") or ""),
        subtitle=str(data.get("subtitle") or ""),
        status=str(data.get("status") or "pending"),
        percent=int(data.get("percent") or 0),
        step=str(data.get("step") or "En attente…"),
        steps_log=list(data.get("steps_log") or []),
        error=str(data.get("error") or ""),
        download_path=Path(data["download_path"]) if data.get("download_path") else None,
        download_name=str(data.get("download_name") or ""),
        archive_id=str(data.get("archive_id") or ""),
        state_path=state_path,
        updated_at=str(data.get("updated_at") or datetime.now().isoformat(timespec="seconds")),
    )
    with JOBS_LOCK:
        JOBS[job.id] = job
    return job


def job_get(job_id: str) -> Job | None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if job is not None:
        return job
    return load_job_from_disk(job_id)


def list_disk_jobs(*, active_only: bool = False, limit: int = 20) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for state_path in JOB_DIR.glob(f"*/runs/*/{JOB_STATE_NAME}"):
        try:
            data = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        status = str(data.get("status") or "pending")
        if active_only and status not in {"pending", "running"}:
            continue
        jobs.append({
            "id": str(data.get("id") or state_path.parent.name),
            "session_id": str(data.get("session_id") or ""),
            "title": str(data.get("title") or ""),
            "subtitle": str(data.get("subtitle") or ""),
            "status": status,
            "percent": int(data.get("percent") or 0),
            "step": str(data.get("step") or ""),
            "download_name": str(data.get("download_name") or ""),
            "archive_id": str(data.get("archive_id") or ""),
            "updated_at": str(data.get("updated_at") or ""),
        })
    jobs.sort(key=lambda item: item.get("updated_at") or "", reverse=True)
    return jobs[:limit]


def job_set(job_id: str, **kwargs: Any) -> None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if job is None:
            return
        for key, value in kwargs.items():
            setattr(job, key, value)
        if "step" in kwargs and kwargs["step"]:
            job.steps_log.append(kwargs["step"])
        job.updated_at = datetime.now().isoformat(timespec="seconds")
        persist_job(job)


def job_snapshot(job_id: str) -> dict[str, Any] | None:
    job = job_get(job_id)
    if job is None:
        return None
    return {
        "id": job.id,
        "session_id": job.session_id,
        "title": job.title,
        "subtitle": job.subtitle,
        "status": job.status,
        "percent": job.percent,
        "step": job.step,
        "steps_log": list(job.steps_log),
        "error": job.error,
        "download_name": job.download_name,
        "archive_id": job.archive_id,
        "updated_at": job.updated_at,
    }


def archive_generated_outputs(
    generated_files: list[Path],
    final_path: Path,
    year: int,
    groups: list[Any],
    selected_chains: list[str],
    selected_natures: list[str],
    mode: str,
    source_workbook: str,
    batch_id: str,
) -> str:
    group_file_index: dict[str, tuple[str, str]] = {}
    for group in groups:
        legacy_name = f"{slugify(group.chain)}_{slugify(group.nature)}_{year}.pdf"
        themed_name = thematic_group_pdf_name(year, group.chain, group.nature)
        group_file_index[legacy_name] = (group.chain, group.nature)
        group_file_index[themed_name] = (group.chain, group.nature)

    ordered_paths: list[Path] = []
    seen: set[str] = set()
    for path in [*generated_files, final_path]:
        resolved = str(path.resolve()) if path.exists() else str(path)
        if resolved in seen:
            continue
        seen.add(resolved)
        ordered_paths.append(path)

    final_archive_id = ""
    final_resolved = str(final_path.resolve()) if final_path.exists() else str(final_path)
    for path in ordered_paths:
        if not path.exists():
            continue

        resolved = str(path.resolve())
        artifact_kind = "report"
        entry_mode = mode
        chains = list(selected_chains)
        natures = list(selected_natures)
        group_chain = ""
        group_nature = ""

        if resolved == final_resolved:
            if path.suffix.lower() == ".zip":
                artifact_kind = "bundle_zip"
            elif path.suffix.lower() in {".xlsx", ".xlsm"}:
                artifact_kind = "excel_dashboard"
                entry_mode = "excel"
            elif path.suffix.lower() == ".pdf":
                artifact_kind = "combined_pdf"
                entry_mode = "combined"
        elif path.suffix.lower() == ".pdf" and path.name.startswith("rapport_") and path.name.endswith("_complet.pdf"):
            artifact_kind = "combined_pdf"
            entry_mode = "combined"
        elif path.suffix.lower() == ".pdf":
            artifact_kind = "group_pdf"
            entry_mode = "grouped"
            group_chain, group_nature = group_file_index.get(path.name, ("", ""))
            if group_chain:
                chains = [group_chain]
            if group_nature:
                natures = [group_nature]

        archive_id = archive_report(
            final_path=path,
            filename=path.name,
            year=year,
            chains=chains,
            natures=natures,
            mode=entry_mode,
            source_workbook=source_workbook,
            archive_dir=ARCHIVE_DIR,
            batch_id=batch_id,
            artifact_kind=artifact_kind,
            group_chain=group_chain,
            group_nature=group_nature,
        )
        if resolved == final_resolved:
            final_archive_id = archive_id
    return final_archive_id


def build_export_tasks(
    dataset: Any,
    year: int,
    selected_chains: list[str],
    selected_natures: list[str],
    variant: str,
) -> list[ExportTask]:
    groups = dataset.available_groups(
        year=year,
        chains=selected_chains or None,
        natures=selected_natures or list(DEFAULT_FOCUS_NATURES),
    )
    if not groups:
        return []

    if variant == "by_chain":
        return [
            ExportTask(
                slug=slugify(chain).lower(),
                label=f"Chaîne {chain}",
                groups=[group for group in groups if group.chain == chain],
            )
            for chain in sorted({group.chain for group in groups})
        ]

    if variant == "by_nature":
        return [
            ExportTask(
                slug=slugify(nature).lower(),
                label=f"Catégorie {nature}",
                groups=[group for group in groups if group.nature == nature],
            )
            for nature in sorted({group.nature for group in groups})
        ]

    if variant == "by_group":
        return [
            ExportTask(
                slug=f"{slugify(group.chain).lower()}_{slugify(group.nature).lower()}",
                label=f"{group.chain} / {group.nature}",
                groups=[group],
            )
            for group in groups
        ]

    return [ExportTask(slug="selection", label="Sélection", groups=groups)]


def rename_task_outputs(paths: list[Path], task: ExportTask, year: int) -> list[Path]:
    group_lookup = {
        f"{slugify(group.chain)}_{slugify(group.nature)}_{year}.pdf": thematic_group_pdf_name(year, group.chain, group.nature)
        for group in task.groups
    }
    task_chains = sorted({group.chain for group in task.groups})
    task_natures = sorted({group.nature for group in task.groups})
    renamed: list[Path] = []
    default_combined_name = f"rapport_arrets_{year}_complet.pdf"
    for path in paths:
        if path.name == default_combined_name:
            target = path.with_name(thematic_combined_pdf_name(year, task_chains, task_natures))
        elif path.name in group_lookup:
            target = path.with_name(group_lookup[path.name])
        else:
            renamed.append(path)
            continue
        if target.exists():
            target.unlink()
        path.rename(target)
        renamed.append(target)
    return renamed


def summarize_scope(values: list[str], fallback: str, limit: int = 2) -> str:
    cleaned = [str(value).strip() for value in values if str(value).strip()]
    if not cleaned:
        return fallback
    if len(cleaned) <= limit:
        return " + ".join(cleaned)
    return " + ".join(cleaned[:limit]) + f" +{len(cleaned) - limit}"


def build_job_title(selected_chains: list[str], selected_natures: list[str], mode: str, variant: str) -> str:
    scope = f"{summarize_scope(selected_chains, 'Toutes chaînes')} · {summarize_scope(selected_natures, 'Toutes catégories')}"
    if variant == "by_chain":
        return f"Pack par chaîne · {scope}"
    if variant == "by_nature":
        return f"Pack par catégorie · {scope}"
    if variant == "by_group":
        return f"Pack par groupe · {scope}"
    if mode == "grouped":
        return f"Export groupes · {scope}"
    if mode == "both":
        return f"Export mixte · {scope}"
    return f"Rapport complet · {scope}"


def build_job_subtitle(year: int | None, mode: str, variant: str) -> str:
    mode_label = {
        "combined": "document complet",
        "grouped": "un PDF par groupe",
        "both": "pack mixte",
    }.get(mode, "export")
    variant_label = {
        "standard": "sélection directe",
        "by_chain": "découpage par chaîne",
        "by_nature": "découpage par catégorie",
        "by_group": "découpage par groupe",
    }.get(variant, variant)
    return f"Année {year or 'auto'} · {mode_label} · {variant_label}"


def run_generation(
    job_id: str,
    workbook_path: Path,
    year: int | None,
    mode: str,
    variant: str,
    selected_chains: list[str],
    selected_natures: list[str],
    equipment_filters: list[str],
    include_overview: bool,
    output_dir: Path,
    session_path: Path,
    run_path: Path,
) -> None:
    def callback(percent: int, message: str) -> None:
        job_set(job_id, percent=percent, step=message)

    with GEN_LOCK:
        try:
            logger.info("Génération lancée: %s", job_id)
            job_set(job_id, status="running", percent=3, step="Chargement du classeur Excel…")
            dataset = get_workbook_dataset(workbook_path)

            resolved_year = year
            if resolved_year is None:
                resolved_year = dataset.available_years[-1]
            elif resolved_year not in dataset.available_years:
                job_set(
                    job_id,
                    status="error",
                    error=f"Année {resolved_year} absente. Disponibles : {', '.join(str(item) for item in dataset.available_years)}.",
                )
                return

            job_set(job_id, percent=8, step="Analyse des groupes disponibles…")
            tasks = build_export_tasks(
                dataset=dataset,
                year=resolved_year,
                selected_chains=selected_chains,
                selected_natures=selected_natures,
                variant=variant,
            )
            if not tasks:
                job_set(job_id, status="error", error="Aucun groupe ne correspond aux filtres sélectionnés.")
                return
            all_groups = [group for task in tasks for group in task.groups]

            output_dir.mkdir(parents=True, exist_ok=True)
            generated: list[Path] = []
            task_count = len(tasks)
            for index, task in enumerate(tasks, start=1):
                task_output_dir = output_dir if task.slug == "selection" else output_dir / task.slug
                task_output_dir.mkdir(parents=True, exist_ok=True)
                task_start = 10 + int(78 * (index - 1) / max(task_count, 1))
                task_end = 10 + int(78 * index / max(task_count, 1))

                def task_callback(percent: int, message: str, *, start: int = task_start, end: int = task_end, label: str = task.label) -> None:
                    scaled = start + int((max(end - start, 1) * percent) / 100)
                    job_set(job_id, percent=min(scaled, max(end, start)), step=f"{label} · {message}")

                job_set(job_id, percent=task_start, step=f"Sous-export {index}/{task_count} — {task.label}")
                task_generated = generate_reports(
                    dataset=dataset,
                    output_dir=task_output_dir,
                    year=resolved_year,
                    mode=mode,
                    groups=task.groups,
                    equipments_filter=equipment_filters or None,
                    include_overview=include_overview,
                    grouped_subdirs=(mode != "combined" and task_count == 1 and task.slug == "selection"),
                    progress_cb=callback if task_count == 1 and task.slug == "selection" else task_callback,
                )
                generated.extend(rename_task_outputs(task_generated, task, resolved_year))

            if not generated:
                job_set(job_id, status="error", error="Aucun fichier n'a été produit.")
                return

            job_set(job_id, percent=99, step="Préparation du téléchargement…")

            direct_pdf = (
                variant == "standard"
                and mode == "combined"
                and len(generated) == 1
                and generated[0].suffix == ".pdf"
            )
            if direct_pdf:
                final_path = generated[0]
                final_name = thematic_combined_pdf_name(resolved_year, selected_chains, selected_natures)
                if final_path.name != final_name:
                    themed_path = final_path.with_name(final_name)
                    if themed_path.exists():
                        themed_path.unlink()
                    final_path.rename(themed_path)
                    final_path = themed_path
            else:
                metadata = read_excel_metadata(workbook_path)
                final_name = thematic_bundle_name(
                    year=resolved_year,
                    chains=selected_chains,
                    natures=selected_natures,
                    mode=mode,
                    variant=variant,
                )
                final_path = run_path / final_name
                if final_path.exists():
                    final_path.unlink()
                build_download_bundle(
                    zip_path=final_path,
                    output_dir=output_dir,
                    workbook_name=resolve_session_workbook_name(session_path) or workbook_path.name,
                    metadata=metadata,
                    year=resolved_year,
                    mode=f"{mode} / {variant}",
                )

            job_set(
                job_id,
                status="done",
                percent=100,
                step="Rapport généré avec succès ✓",
                download_path=final_path,
                download_name=final_name,
            )
            logger.info("Génération terminée: %s -> %s", job_id, final_name)

            try:
                archive_id = archive_generated_outputs(
                    generated_files=generated,
                    final_path=final_path,
                    year=resolved_year,
                    groups=all_groups,
                    selected_chains=selected_chains,
                    selected_natures=selected_natures,
                    mode=mode,
                    source_workbook=resolve_session_workbook_name(session_path) or workbook_path.name,
                    batch_id=job_id,
                )
                job_set(job_id, archive_id=archive_id)
            except Exception:
                logger.exception("Archivage échoué après génération pour %s", job_id)
                pass

        except MemoryError:
            logger.exception("Mémoire insuffisante pendant la génération %s", job_id)
            job_set(job_id, status="error", error="Mémoire insuffisante. Réduisez le nombre de groupes.")
        except Exception as exc:
            logger.exception("Erreur de génération pour %s", job_id)
            job_set(job_id, status="error", error=str(exc))
