from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import Response

from .config import (
    ACTIVE_SESSION_COOKIE,
    ALLOWED_EXTENSIONS,
    DB_PATH,
    JOB_DIR,
    SESSION_META_NAME,
    SESSION_SOURCE_BASENAME,
    SETTINGS_PATH,
)
from .logging_setup import get_logger
from .utils import build_export_label

logger = get_logger("storage")


SETTINGS_DEFAULTS: dict[str, Any] = {
    "smtp_host": "mail.perenkap.com",
    "smtp_port": 587,
    "smtp_user": "noreply@perenkap.com",
    "smtp_pass": "",
    "smtp_from": "noreply@perenkap.com",
    "smtp_from_name": "RAPPORT PDF",
    "smtp_use_tls": True,
    "smtp_use_ssl": False,
    "default_recipients": [],
    "subject_template": "Rapport Arrêts {label} — {year}",
    "email_signature": "Ce rapport a été généré automatiquement par HR Brasserie Reports.",
}


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(DB_PATH))
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS reports (
            id            TEXT PRIMARY KEY,
            filename      TEXT NOT NULL,
            label         TEXT DEFAULT '',
            year          INTEGER,
            chains        TEXT DEFAULT '[]',
            natures       TEXT DEFAULT '[]',
            mode          TEXT DEFAULT 'combined',
            created_at    TEXT NOT NULL,
            source_workbook TEXT DEFAULT '',
            size_bytes    INTEGER DEFAULT 0,
            filepath      TEXT NOT NULL
        )
        """
    )
    report_columns = {row[1] for row in con.execute("PRAGMA table_info(reports)").fetchall()}
    report_migrations = {
        "batch_id": "TEXT DEFAULT ''",
        "artifact_kind": "TEXT DEFAULT 'report'",
        "group_chain": "TEXT DEFAULT ''",
        "group_nature": "TEXT DEFAULT ''",
    }
    for column, ddl in report_migrations.items():
        if column not in report_columns:
            con.execute(f"ALTER TABLE reports ADD COLUMN {column} {ddl}")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS email_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id  TEXT NOT NULL,
            sent_at    TEXT NOT NULL,
            recipients TEXT NOT NULL,
            subject    TEXT DEFAULT ''
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS download_log (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id     TEXT NOT NULL,
            downloaded_at TEXT NOT NULL,
            source        TEXT DEFAULT ''
        )
        """
    )
    con.commit()
    con.close()
    logger.info("Base SQLite initialisée automatiquement: %s", DB_PATH)


def fmt_size(size_bytes: int) -> str:
    if size_bytes >= 1_048_576:
        return f"{size_bytes / 1_048_576:.1f} Mo"
    if size_bytes >= 1024:
        return f"{size_bytes / 1024:.0f} Ko"
    return f"{size_bytes} o"


def artifact_label(kind: str) -> str:
    return {
        "combined_pdf": "PDF complet",
        "group_pdf": "PDF groupe",
        "bundle_zip": "Archive ZIP",
        "excel_dashboard": "Export Excel",
    }.get(kind, "Rapport")


def mode_label(mode: str) -> str:
    return {
        "combined": "Document complet",
        "grouped": "Par groupe",
        "both": "Mixte",
        "excel": "Excel",
    }.get(mode, mode or "Inconnu")


def archive_report(
    final_path: Path,
    filename: str,
    year: int,
    chains: list[str],
    natures: list[str],
    mode: str,
    source_workbook: str,
    archive_dir: Path,
    batch_id: str = "",
    artifact_kind: str = "report",
    group_chain: str = "",
    group_nature: str = "",
) -> str:
    import shutil
    import uuid

    archive_id = uuid.uuid4().hex
    dest = archive_dir / f"{archive_id}_{filename}"
    shutil.copy2(final_path, dest)
    size = dest.stat().st_size
    con = sqlite3.connect(str(DB_PATH))
    con.execute(
        """INSERT INTO reports
           (id, filename, label, year, chains, natures, mode, created_at, source_workbook,
            size_bytes, filepath, batch_id, artifact_kind, group_chain, group_nature)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            archive_id,
            filename,
            build_export_label(chains, natures, mode),
            year,
            json.dumps(chains, ensure_ascii=False),
            json.dumps(natures, ensure_ascii=False),
            mode,
            datetime.now().isoformat(timespec="seconds"),
            source_workbook,
            size,
            str(dest),
            batch_id,
            artifact_kind,
            group_chain,
            group_nature,
        ),
    )
    con.commit()
    con.close()
    logger.info("Archive créée: %s (%s)", filename, archive_id)
    return archive_id


def log_email_send(report_id: str, recipients: list[str], subject: str) -> None:
    con = sqlite3.connect(str(DB_PATH))
    con.execute(
        "INSERT INTO email_log (report_id, sent_at, recipients, subject) VALUES (?,?,?,?)",
        (
            report_id,
            datetime.now().isoformat(timespec="seconds"),
            json.dumps(recipients, ensure_ascii=False),
            subject,
        ),
    )
    con.commit()
    con.close()
    logger.info("Envoi email journalisé pour %s vers %s", report_id, ", ".join(recipients))


def log_download(report_id: str, source: str = "") -> None:
    con = sqlite3.connect(str(DB_PATH))
    con.execute(
        "INSERT INTO download_log (report_id, downloaded_at, source) VALUES (?,?,?)",
        (report_id, datetime.now().isoformat(timespec="seconds"), source),
    )
    con.commit()
    con.close()
    logger.info("Téléchargement journalisé pour %s via %s", report_id, source or "inconnu")


def list_recent_downloads(limit: int = 10) -> list[dict[str, Any]]:
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    rows = con.execute(
        """
        SELECT dl.report_id, dl.downloaded_at, dl.source, r.filename, r.artifact_kind,
               r.mode, r.group_chain, r.group_nature, r.filepath
        FROM download_log dl
        LEFT JOIN reports r ON r.id = dl.report_id
        ORDER BY dl.downloaded_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["artifact_label"] = artifact_label(item.get("artifact_kind") or "report")
        item["mode_label"] = mode_label(item.get("mode") or "")
        item["exists"] = bool(item.get("filepath")) and Path(item["filepath"]).exists()
        result.append(item)
    con.close()
    return result


def list_archive() -> list[dict[str, Any]]:
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM reports ORDER BY created_at DESC").fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["chains"] = json.loads(item.get("chains") or "[]")
        item["natures"] = json.loads(item.get("natures") or "[]")
        item["email_count"] = con.execute(
            "SELECT COUNT(*) FROM email_log WHERE report_id=?",
            (item["id"],),
        ).fetchone()[0]
        item["last_sent_at"] = con.execute(
            "SELECT MAX(sent_at) FROM email_log WHERE report_id=?",
            (item["id"],),
        ).fetchone()[0] or ""
        raw_batch_id = item.get("batch_id") or ""
        item["batch_id"] = raw_batch_id or item["id"]
        if raw_batch_id:
            item["batch_count"] = con.execute(
                "SELECT COUNT(*) FROM reports WHERE batch_id=?",
                (raw_batch_id,),
            ).fetchone()[0]
        else:
            item["batch_count"] = 1
        item["exists"] = Path(item["filepath"]).exists()
        item["size_human"] = fmt_size(item.get("size_bytes") or 0)
        item["artifact_label"] = artifact_label(item.get("artifact_kind") or "report")
        item["mode_label"] = mode_label(item.get("mode") or "")
        if item.get("group_chain") and item.get("group_nature"):
            item["group_label"] = f"{item['group_chain']} / {item['group_nature']}"
        else:
            item["group_label"] = item.get("label") or "Global"
        result.append(item)
    con.close()
    return result


def get_archive_entry(archive_id: str) -> dict[str, Any] | None:
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    row = con.execute("SELECT * FROM reports WHERE id=?", (archive_id,)).fetchone()
    if row is None:
        con.close()
        return None
    item = dict(row)
    item["chains"] = json.loads(item.get("chains") or "[]")
    item["natures"] = json.loads(item.get("natures") or "[]")
    logs = con.execute(
        "SELECT * FROM email_log WHERE report_id=? ORDER BY sent_at DESC",
        (archive_id,),
    ).fetchall()
    item["email_logs"] = []
    for log in logs:
        current = dict(log)
        current["recipients"] = json.loads(current.get("recipients") or "[]")
        item["email_logs"].append(current)
    con.close()
    return item


def delete_archive_entry(archive_id: str) -> bool:
    entry = get_archive_entry(archive_id)
    if entry is None:
        return False
    Path(entry["filepath"]).unlink(missing_ok=True)
    con = sqlite3.connect(str(DB_PATH))
    con.execute("DELETE FROM email_log WHERE report_id=?", (archive_id,))
    con.execute("DELETE FROM reports WHERE id=?", (archive_id,))
    con.commit()
    con.close()
    logger.info("Archive supprimée: %s", archive_id)
    return True


def delete_archive_batch(batch_id: str) -> int:
    con = sqlite3.connect(str(DB_PATH))
    rows = con.execute("SELECT id FROM reports WHERE batch_id=?", (batch_id,)).fetchall()
    con.close()
    deleted = 0
    for row in rows:
        if delete_archive_entry(row[0]):
            deleted += 1
    return deleted


def load_settings() -> dict[str, Any]:
    if SETTINGS_PATH.exists():
        try:
            data = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
            return {**SETTINGS_DEFAULTS, **data}
        except Exception:
            pass
    return dict(SETTINGS_DEFAULTS)


def save_settings(data: dict[str, Any]) -> None:
    merged = {**SETTINGS_DEFAULTS, **load_settings(), **data}
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS_PATH.write_text(json.dumps(merged, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Paramètres sauvegardés dans %s", SETTINGS_PATH)


def session_meta_path(job_path: Path) -> Path:
    return job_path / SESSION_META_NAME


def save_session_metadata(job_path: Path, source_storage_name: str, source_display_name: str) -> None:
    payload = {
        "source_storage_name": source_storage_name,
        "source_display_name": source_display_name,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    session_meta_path(job_path).write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("Session sauvegardée: %s", source_display_name)


def load_session_metadata(job_path: Path) -> dict[str, Any]:
    meta_path = session_meta_path(job_path)
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def resolve_session_workbook(job_path: Path) -> Path | None:
    meta = load_session_metadata(job_path)
    stored_name = str(meta.get("source_storage_name") or "").strip()
    if stored_name:
        candidate = job_path / stored_name
        if candidate.exists():
            return candidate

    for extension in sorted(ALLOWED_EXTENSIONS):
        candidate = job_path / f"{SESSION_SOURCE_BASENAME}{extension}"
        if candidate.exists():
            return candidate

    candidates = [
        path
        for path in sorted(job_path.iterdir())
        if path.is_file()
        and path.suffix.lower() in ALLOWED_EXTENSIONS
        and not path.name.startswith(("dashboard_", "rapport_"))
    ]
    return candidates[0] if candidates else None


def resolve_session_workbook_name(job_path: Path) -> str:
    meta = load_session_metadata(job_path)
    name = str(meta.get("source_display_name") or "").strip()
    if name:
        return name
    workbook_path = resolve_session_workbook(job_path)
    return workbook_path.name if workbook_path is not None else ""


def list_import_sessions(limit: int = 10) -> list[dict[str, Any]]:
    sessions: list[dict[str, Any]] = []
    for meta_path in JOB_DIR.glob(f"*/{SESSION_META_NAME}"):
        job_path = meta_path.parent
        workbook_path = resolve_session_workbook(job_path)
        if workbook_path is None or not workbook_path.exists():
            continue
        meta = load_session_metadata(job_path)
        created_at = str(meta.get("created_at") or "")
        sessions.append({
            "job_id": job_path.name,
            "filename": str(meta.get("source_display_name") or workbook_path.name),
            "created_at": created_at,
            "size_human": fmt_size(workbook_path.stat().st_size),
            "filepath": str(workbook_path),
        })
    sessions.sort(key=lambda item: item.get("created_at") or "", reverse=True)
    return sessions[:limit]


def attach_active_session_cookie(response: Response, session_id: str) -> Response:
    response.set_cookie(
        ACTIVE_SESSION_COOKIE,
        session_id,
        max_age=60 * 60 * 24 * 30,
        httponly=False,
        samesite="Lax",
    )
    return response
