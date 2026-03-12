from __future__ import annotations

from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
JOB_DIR = BASE_DIR / ".jobs"
ARCHIVE_DIR = BASE_DIR / "archive"
LOG_DIR = BASE_DIR / "logs"
DB_PATH = BASE_DIR / "hr_archive.db"
SETTINGS_PATH = BASE_DIR / "hr_settings.json"

SESSION_META_NAME = "session.json"
SESSION_SOURCE_BASENAME = "source_workbook"
JOB_STATE_NAME = "job.json"
ACTIVE_SESSION_COOKIE = "hr_active_session"
ANALYSIS_CACHE_DIRNAME = ".analysis_cache"
DATASET_SNAPSHOT_NAME = "dataset_snapshot.pkl"
DATASET_SNAPSHOT_META_NAME = "dataset_snapshot.json"

MAX_UPLOAD_MB = 100
ALLOWED_EXTENSIONS = {".xlsx", ".xlsm"}

APP_XML_NS = {
    "app": "http://schemas.openxmlformats.org/officeDocument/2006/extended-properties",
}
CORE_XML_NS = {
    "cp": "http://schemas.openxmlformats.org/package/2006/metadata/core-properties",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "dcmitype": "http://purl.org/dc/dcmitype/",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}

def ensure_runtime_dirs() -> None:
    for path in (JOB_DIR, ARCHIVE_DIR, LOG_DIR):
        path.mkdir(parents=True, exist_ok=True)


ensure_runtime_dirs()
