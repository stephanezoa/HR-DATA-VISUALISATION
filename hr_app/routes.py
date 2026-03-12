from __future__ import annotations

import re
import shutil
import smtplib
import threading
import uuid
from pathlib import Path
from typing import Any

import pandas as pd
from flask import Flask, Response, flash, jsonify, make_response, redirect, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

from generate_arrets_reports import DEFAULT_FOCUS_NATURES, ensure_theme

from .analysis import (
    build_calendar_payload,
    build_inspection_payload,
    build_pareto_payload,
    build_trend_payload,
    get_cached_analysis,
    get_workbook_dataset,
    get_workbook_metadata,
    start_analysis_prewarm,
)
from .config import (
    ACTIVE_SESSION_COOKIE,
    ALLOWED_EXTENSIONS,
    ARCHIVE_DIR,
    BASE_DIR,
    JOB_DIR,
    JOB_STATE_NAME,
    MAX_UPLOAD_MB,
    SESSION_SOURCE_BASENAME,
)
from .jobs import (
    JOBS,
    JOBS_LOCK,
    Job,
    build_job_subtitle,
    build_job_title,
    job_get,
    job_snapshot,
    list_disk_jobs,
    persist_job,
    run_generation,
)
from .logging_setup import get_logger, setup_logging
from .mail import send_pdf_email, test_smtp_connection
from .storage import (
    archive_report,
    attach_active_session_cookie,
    delete_archive_batch,
    delete_archive_entry,
    get_archive_entry,
    init_db,
    list_archive,
    list_import_sessions,
    list_recent_downloads,
    load_settings,
    log_download,
    log_email_send,
    resolve_session_workbook,
    resolve_session_workbook_name,
    save_session_metadata,
    save_settings,
)
from .utils import build_excel_export, normalize_checkbox_list, parse_equipment_filters

app_logger = get_logger("app")
routes_logger = get_logger("routes")


def render_session_inspection(job_id: str, workbook_path: Path, filename: str | None = None) -> str:
    actual_name = filename or resolve_session_workbook_name(workbook_path.parent) or workbook_path.name
    job_path = workbook_path.parent
    metadata = get_workbook_metadata(job_path, workbook_path)
    dataset = get_workbook_dataset(workbook_path)
    cached = get_cached_analysis(
        job_path=job_path,
        workbook_path=workbook_path,
        namespace="inspect",
        params={"filename": actual_name},
        builder=lambda: build_inspection_payload(dataset),
    )
    start_analysis_prewarm(job_id, workbook_path)
    group_rows = cached["group_rows"]
    settings = load_settings()
    return render_template(
        "inspect.html",
        job_id=job_id,
        filename=actual_name,
        metadata=metadata,
        years=cached["years"],
        default_year=cached["default_year"],
        default_mode="combined",
        chains=sorted({row["chain"] for row in group_rows}),
        natures=sorted({row["nature"] for row in group_rows}),
        groups=group_rows,
        default_recipients=settings.get("default_recipients", []),
    )


def create_app() -> Flask:
    ensure_theme()
    setup_logging()
    init_db()

    app = Flask(
        __name__,
        template_folder=str(BASE_DIR / "templates"),
        static_folder=str(BASE_DIR / "static"),
        static_url_path="/static",
    )
    app.config["SECRET_KEY"] = "hr-brasserie-local"
    app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_MB * 1024 * 1024
    app_logger.info("Application Flask initialisée")

    @app.context_processor
    def inject_ui_defaults() -> dict[str, Any]:
        settings = load_settings()
        return {"ui_default_recipients": settings.get("default_recipients", [])}

    def wants_json() -> bool:
        path = request.path
        return path.startswith(("/api/", "/generate", "/download", "/export-excel"))

    @app.errorhandler(413)
    def too_large(_error):
        if wants_json():
            return jsonify(error=f"Fichier trop volumineux (max {MAX_UPLOAD_MB} Mo)."), 413
        flash(f"Le fichier dépasse {MAX_UPLOAD_MB} Mo.", "error")
        return redirect(url_for("index"))

    @app.errorhandler(404)
    def not_found(_error):
        if wants_json():
            return jsonify(error="Ressource introuvable."), 404
        return render_template("error.html", code=404, message="Page introuvable."), 404

    @app.errorhandler(500)
    def server_error(_error):
        if wants_json():
            return jsonify(error="Erreur serveur inattendue."), 500
        return render_template("error.html", code=500, message="Erreur serveur. Rechargez et réessayez."), 500

    @app.get("/")
    def index():
        imports = list_import_sessions(limit=10)
        active_session_id = (request.cookies.get(ACTIVE_SESSION_COOKIE) or "").strip()
        active_session = next((item for item in imports if item["job_id"] == active_session_id), None)
        if active_session is None and imports:
            active_session = imports[0]
        recent_entries = [entry for entry in list_archive() if entry.get("exists")][:8]
        recent_downloads = list_recent_downloads(limit=10)
        return render_template(
            "index.html",
            active_session=active_session,
            recent_entries=recent_entries,
            recent_downloads=recent_downloads,
            recent_imports=imports,
        )

    @app.get("/import")
    def import_page():
        return render_template("upload.html", max_mb=MAX_UPLOAD_MB)

    @app.get("/inspect/<job_id>")
    def inspect_saved_session(job_id: str):
        if not re.fullmatch(r"[0-9a-f]{32}", job_id):
            flash("Session invalide.", "error")
            return redirect(url_for("index"))
        job_path = JOB_DIR / job_id
        workbook_path = resolve_session_workbook(job_path)
        if workbook_path is None:
            flash("Fichier source introuvable pour cette session.", "error")
            return redirect(url_for("index"))
        try:
            response = make_response(
                render_session_inspection(
                    job_id=job_id,
                    workbook_path=workbook_path,
                    filename=resolve_session_workbook_name(job_path),
                )
            )
            return attach_active_session_cookie(response, job_id)
        except Exception as exc:  # noqa: BLE001
            flash(f"Impossible de rouvrir cet import : {exc}", "error")
            return redirect(url_for("index"))

    @app.post("/inspect")
    def inspect_workbook():
        workbook = request.files.get("workbook")
        if not workbook or not (workbook.filename or "").strip():
            flash("Sélectionne un fichier Excel (.xlsx ou .xlsm).", "error")
            return redirect(url_for("import_page"))

        extension = Path(workbook.filename).suffix.lower()
        if extension not in ALLOWED_EXTENSIONS:
            flash(f"Format non supporté « {extension} ». Acceptés : .xlsx, .xlsm.", "error")
            return redirect(url_for("import_page"))

        job_id = uuid.uuid4().hex
        job_path = JOB_DIR / job_id
        job_path.mkdir(parents=True, exist_ok=True)

        filename = secure_filename(workbook.filename) or f"workbook{extension}"
        source_name = f"{SESSION_SOURCE_BASENAME}{extension}"
        workbook_path = job_path / source_name
        try:
            workbook.save(workbook_path)
            save_session_metadata(job_path, source_name, filename)
            routes_logger.info("Classeur importé: %s (%s)", filename, job_id)
        except OSError as exc:
            shutil.rmtree(job_path, ignore_errors=True)
            flash(f"Impossible de sauvegarder le fichier : {exc}", "error")
            return redirect(url_for("import_page"))

        try:
            response = make_response(
                render_session_inspection(
                    job_id=job_id,
                    workbook_path=workbook_path,
                    filename=filename,
                )
            )
            return attach_active_session_cookie(response, job_id)
        except ValueError as exc:
            shutil.rmtree(job_path, ignore_errors=True)
            flash(f"Structure non reconnue : {exc}", "error")
            return redirect(url_for("import_page"))
        except Exception as exc:  # noqa: BLE001
            shutil.rmtree(job_path, ignore_errors=True)
            flash(f"Impossible d'analyser le classeur : {exc}", "error")
            return redirect(url_for("import_page"))

    @app.post("/generate")
    def generate():
        try:
            session_id = (request.form.get("job_id") or "").strip()
            if not session_id or not re.fullmatch(r"[0-9a-f]{32}", session_id):
                return jsonify(error="Session invalide. Recharge le fichier Excel."), 400

            job_path = JOB_DIR / session_id
            if not job_path.exists():
                return jsonify(error="Session expirée. Recharge le fichier Excel."), 400

            workbook_path = resolve_session_workbook(job_path)
            if workbook_path is None:
                return jsonify(error="Fichier source introuvable dans la session."), 400

            year_raw = (request.form.get("year") or "").strip()
            try:
                year = int(year_raw) if year_raw else None
            except ValueError:
                return jsonify(error=f"Année invalide : « {year_raw} »."), 400

            mode = request.form.get("mode", "combined")
            if mode not in {"grouped", "combined", "both"}:
                mode = "combined"

            variant = request.form.get("variant", "standard")
            if variant not in {"standard", "by_chain", "by_nature", "by_group"}:
                variant = "standard"

            selected_chains = normalize_checkbox_list(request.form.getlist("chains"))
            selected_natures = normalize_checkbox_list(request.form.getlist("natures"))
            equipment_filters = parse_equipment_filters(request.form.get("equipments", ""))
            include_overview = request.form.get("include_overview") == "on"
            job_title = build_job_title(selected_chains, selected_natures, mode, variant)
            job_subtitle = build_job_subtitle(year, mode, variant)

            run_id = uuid.uuid4().hex
            run_path = job_path / "runs" / run_id
            output_dir = run_path / "output"
            state_path = run_path / JOB_STATE_NAME
            try:
                if run_path.exists():
                    shutil.rmtree(run_path)
                run_path.mkdir(parents=True, exist_ok=True)
                output_dir.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                return jsonify(error=f"Impossible de préparer le dossier de sortie : {exc}"), 500

            new_job = Job(id=run_id, session_id=session_id, title=job_title, subtitle=job_subtitle, state_path=state_path)
            with JOBS_LOCK:
                JOBS[run_id] = new_job
                persist_job(new_job)

            thread = threading.Thread(
                target=run_generation,
                args=(
                    run_id,
                    workbook_path,
                    year,
                    mode,
                    variant,
                    selected_chains,
                    selected_natures,
                    equipment_filters,
                    include_overview,
                    output_dir,
                    job_path,
                    run_path,
                ),
                daemon=True,
            )
            thread.start()
            routes_logger.info("Job d'export lancé: %s pour session %s", run_id, session_id)
            return jsonify(job_id=run_id, session_id=session_id, title=job_title, subtitle=job_subtitle)
        except Exception as exc:  # noqa: BLE001
            routes_logger.exception("Erreur inattendue sur /generate")
            return jsonify(error=f"Erreur inattendue : {exc}"), 500

    @app.get("/api/status/<job_id>")
    def api_status(job_id: str):
        if not re.fullmatch(r"[0-9a-f]{32}", job_id):
            return jsonify(error="Job ID invalide."), 400
        snapshot = job_snapshot(job_id)
        if snapshot is None:
            return jsonify(error="Job introuvable."), 404
        return jsonify(snapshot)

    @app.get("/api/jobs")
    def api_jobs():
        active_only = request.args.get("status") == "active"
        return jsonify(jobs=list_disk_jobs(active_only=active_only))

    @app.get("/download/<job_id>")
    def download(job_id: str):
        if not re.fullmatch(r"[0-9a-f]{32}", job_id):
            flash("Lien invalide.", "error")
            return redirect(url_for("index"))
        snapshot = job_snapshot(job_id)
        if snapshot is None or snapshot["status"] != "done":
            flash("Fichier non disponible.", "error")
            return redirect(url_for("index"))
        job = job_get(job_id)
        path = job.download_path if job else None
        name = job.download_name if job else ""
        if path is None or not path.exists():
            flash("Fichier introuvable. Régénère le rapport.", "error")
            return redirect(url_for("index"))
        if job and job.archive_id:
            try:
                log_download(job.archive_id, source="job")
            except Exception:
                pass
        routes_logger.info("Téléchargement du job %s", job_id)
        return send_file(path, as_attachment=True, download_name=name)

    @app.post("/api/send-email/<job_id>")
    def api_send_email(job_id: str):
        if not re.fullmatch(r"[0-9a-f]{32}", job_id):
            return jsonify(ok=False, error="Job ID invalide."), 400

        snapshot = job_snapshot(job_id)
        if snapshot is None or snapshot["status"] != "done":
            return jsonify(ok=False, error="Rapport non disponible."), 400

        job = job_get(job_id)
        path = job.download_path if job else None
        archive_id = job.archive_id if job else ""
        if path is None or not path.exists():
            return jsonify(ok=False, error="Fichier PDF introuvable."), 400

        raw_to = (request.form.get("to") or "").strip()
        recipients = [email.strip() for email in re.split(r"[;,\n]+", raw_to) if email.strip()]
        if not recipients:
            return jsonify(ok=False, error="Aucune adresse email renseignée."), 400
        invalid = [email for email in recipients if not re.fullmatch(r"[^@]+@[^@]+\.[^@]+", email)]
        if invalid:
            return jsonify(ok=False, error=f"Adresse(s) invalide(s) : {', '.join(invalid)}"), 400

        settings = load_settings()
        if not settings.get("smtp_host") or not settings.get("smtp_user"):
            return jsonify(ok=False, error="SMTP non configuré. Va dans Paramètres."), 400

        subject = settings["subject_template"].format(
            label=snapshot.get("download_name", path.name).removesuffix(".pdf"),
            year="",
        ).strip(" —")
        signature = settings.get("email_signature", "")
        body = f"Veuillez trouver ci-joint le fichier généré pour les arrêts de production.\n\nFichier : {path.name}\n\n{signature}".strip()

        try:
            send_pdf_email(recipients=recipients, pdf_path=path, subject=subject, body=body, settings=settings)
            if archive_id:
                try:
                    log_email_send(archive_id, recipients, subject)
                except Exception:
                    pass
            return jsonify(ok=True, message=f"Envoyé à {', '.join(recipients)}.")
        except smtplib.SMTPAuthenticationError:
            return jsonify(ok=False, error="Authentification SMTP échouée."), 502
        except smtplib.SMTPException as exc:
            return jsonify(ok=False, error=f"Erreur SMTP : {exc}"), 502
        except OSError as exc:
            return jsonify(ok=False, error=f"Connexion impossible : {exc}"), 502

    @app.get("/archive")
    def archive_page():
        entries = list_archive()
        return render_template("archive.html", entries=entries)

    @app.get("/download/archive/<archive_id>")
    def download_archive(archive_id: str):
        if not re.fullmatch(r"[0-9a-f]{32}", archive_id):
            flash("ID invalide.", "error")
            return redirect(url_for("archive_page"))
        entry = get_archive_entry(archive_id)
        if entry is None:
            flash("Rapport introuvable dans l'archive.", "error")
            return redirect(url_for("archive_page"))
        filepath = Path(entry["filepath"])
        if not filepath.exists():
            flash("Fichier supprimé du disque.", "error")
            return redirect(url_for("archive_page"))
        try:
            log_download(archive_id, source="archive")
        except Exception:
            pass
        routes_logger.info("Téléchargement d'archive %s", archive_id)
        return send_file(filepath, as_attachment=True, download_name=entry["filename"])

    @app.post("/api/send-email/archive/<archive_id>")
    def api_send_email_archive(archive_id: str):
        if not re.fullmatch(r"[0-9a-f]{32}", archive_id):
            return jsonify(ok=False, error="ID invalide."), 400
        entry = get_archive_entry(archive_id)
        if entry is None:
            return jsonify(ok=False, error="Rapport introuvable."), 404
        filepath = Path(entry["filepath"])
        if not filepath.exists():
            return jsonify(ok=False, error="Fichier non disponible sur le disque."), 404

        raw_to = (request.form.get("to") or "").strip()
        recipients = [email.strip() for email in re.split(r"[;,\n]+", raw_to) if email.strip()]
        if not recipients:
            return jsonify(ok=False, error="Aucune adresse email."), 400
        invalid = [email for email in recipients if not re.fullmatch(r"[^@]+@[^@]+\.[^@]+", email)]
        if invalid:
            return jsonify(ok=False, error=f"Adresse(s) invalide(s) : {', '.join(invalid)}"), 400

        settings = load_settings()
        if not settings.get("smtp_host") or not settings.get("smtp_user"):
            return jsonify(ok=False, error="SMTP non configuré. Va dans Paramètres."), 400

        subject = settings["subject_template"].format(
            label=entry.get("label") or entry["filename"],
            year=entry.get("year") or "",
        ).strip(" —")
        signature = settings.get("email_signature", "")
        body = f"Veuillez trouver ci-joint le fichier généré pour les arrêts de production.\n\nFichier : {entry['filename']}\n\n{signature}".strip()

        try:
            send_pdf_email(recipients=recipients, pdf_path=filepath, subject=subject, body=body, settings=settings)
            try:
                log_email_send(archive_id, recipients, subject)
            except Exception:
                pass
            return jsonify(ok=True, message=f"Envoyé à {', '.join(recipients)}.")
        except smtplib.SMTPAuthenticationError:
            return jsonify(ok=False, error="Authentification SMTP échouée."), 502
        except smtplib.SMTPException as exc:
            return jsonify(ok=False, error=f"Erreur SMTP : {exc}"), 502
        except OSError as exc:
            return jsonify(ok=False, error=f"Connexion impossible : {exc}"), 502

    @app.post("/api/archive/delete")
    def api_archive_delete():
        data = request.get_json(silent=True) or {}
        archive_id = (data.get("id") or "").strip()
        if not re.fullmatch(r"[0-9a-f]{32}", archive_id):
            return jsonify(ok=False, error="ID invalide."), 400
        ok = delete_archive_entry(archive_id)
        if not ok:
            return jsonify(ok=False, error="Rapport introuvable."), 404
        routes_logger.info("Archive supprimée depuis l'API: %s", archive_id)
        return jsonify(ok=True)

    @app.post("/api/archive/delete-bulk")
    def api_archive_delete_bulk():
        data = request.get_json(silent=True) or {}
        ids = data.get("ids", [])
        deleted = 0
        for archive_id in ids:
            if re.fullmatch(r"[0-9a-f]{32}", archive_id) and delete_archive_entry(archive_id):
                deleted += 1
        return jsonify(ok=True, deleted=deleted)

    @app.post("/api/archive/delete-batch")
    def api_archive_delete_batch():
        data = request.get_json(silent=True) or {}
        batch_id = (data.get("batch_id") or "").strip()
        if not re.fullmatch(r"[0-9a-f]{32}", batch_id):
            return jsonify(ok=False, error="Lot invalide."), 400
        deleted = delete_archive_batch(batch_id)
        if deleted <= 0:
            return jsonify(ok=False, error="Aucun fichier trouvé pour ce lot."), 404
        routes_logger.info("Lot supprimé depuis l'API: %s (%s fichiers)", batch_id, deleted)
        return jsonify(ok=True, deleted=deleted)

    @app.get("/pareto/<job_id>")
    def pareto(job_id: str):
        if not re.fullmatch(r"[0-9a-f]{32}", job_id):
            flash("Session invalide.", "error")
            return redirect(url_for("index"))
        job_path = JOB_DIR / job_id
        workbook_path = resolve_session_workbook(job_path)
        if workbook_path is None:
            flash("Session expirée. Rechargez le fichier Excel.", "error")
            return redirect(url_for("index"))
        try:
            dataset = get_workbook_dataset(workbook_path)
            year_raw = (request.args.get("year") or "").strip()
            try:
                year = int(year_raw) if year_raw else dataset.available_years[-1]
            except ValueError:
                year = dataset.available_years[-1]
            if year not in dataset.available_years:
                year = dataset.available_years[-1]

            selected_chain_values = normalize_checkbox_list([request.args.get("chain", "")])
            selected_nature_values = normalize_checkbox_list([request.args.get("nature", "")])
            selected_chain = selected_chain_values[0] if selected_chain_values else ""
            selected_nature = selected_nature_values[0] if selected_nature_values else ""

            payload = get_cached_analysis(
                job_path=job_path,
                workbook_path=workbook_path,
                namespace="pareto",
                params={"year": year, "chain": selected_chain, "nature": selected_nature},
                builder=lambda: build_pareto_payload(dataset, int(year), selected_chain, selected_nature),
            )

            response = make_response(
                render_template(
                    "pareto.html",
                    job_id=job_id,
                    year=year,
                    years=[int(value) for value in dataset.available_years],
                    chain_options=payload["chain_options"],
                    nature_options=payload["nature_options"],
                    selected_chain=selected_chain,
                    selected_nature=selected_nature,
                    labels=payload["labels"],
                    values=payload["values"],
                    cumul=payload["cumul"],
                    pareto_cut=payload["pareto_cut"],
                    total_equipment=payload["total_equipment"],
                    pareto_share=payload["pareto_share"],
                    peak_equipment=payload["peak_equipment"],
                    peak_value=payload["peak_value"],
                    median_value=payload["median_value"],
                    rows=payload["rows"],
                )
            )
            return attach_active_session_cookie(response, job_id)
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("index"))
        except Exception as exc:  # noqa: BLE001
            flash(f"Erreur Pareto : {exc}", "error")
            return redirect(url_for("index"))

    @app.get("/trend/<job_id>")
    def trend(job_id: str):
        if not re.fullmatch(r"[0-9a-f]{32}", job_id):
            flash("Session invalide.", "error")
            return redirect(url_for("index"))
        job_path = JOB_DIR / job_id
        workbook_path = resolve_session_workbook(job_path)
        if workbook_path is None:
            flash("Session expirée. Rechargez le fichier Excel.", "error")
            return redirect(url_for("index"))
        try:
            dataset = get_workbook_dataset(workbook_path)
            payload = get_cached_analysis(
                job_path=job_path,
                workbook_path=workbook_path,
                namespace="trend",
                params={"view": "default"},
                builder=lambda: build_trend_payload(dataset),
            )
            response = make_response(
                render_template(
                    "trend.html",
                    job_id=job_id,
                    years=payload["years"],
                    next_year=payload["next_year"],
                    chain_datasets=payload["chain_datasets"],
                    nature_datasets=payload["nature_datasets"],
                    best_chain=payload["best_chain"],
                    risk_chain=payload["risk_chain"],
                )
            )
            return attach_active_session_cookie(response, job_id)
        except Exception as exc:  # noqa: BLE001
            flash(f"Erreur Tendance : {exc}", "error")
            return redirect(url_for("index"))

    @app.get("/calendar/<job_id>")
    def calendar(job_id: str):
        if not re.fullmatch(r"[0-9a-f]{32}", job_id):
            flash("Session invalide.", "error")
            return redirect(url_for("index"))
        job_path = JOB_DIR / job_id
        workbook_path = resolve_session_workbook(job_path)
        if workbook_path is None:
            flash("Session expirée. Rechargez le fichier Excel.", "error")
            return redirect(url_for("index"))
        try:
            dataset = get_workbook_dataset(workbook_path)
            payload = get_cached_analysis(
                job_path=job_path,
                workbook_path=workbook_path,
                namespace="calendar",
                params={"view": "default"},
                builder=lambda: build_calendar_payload(dataset),
            )
            response = make_response(
                render_template(
                    "calendar.html",
                    job_id=job_id,
                    year=payload["year"],
                    years=payload["years"],
                    chains=payload["chains"],
                    weeks=payload["weeks"],
                    heatmap=payload["heatmap"],
                    limits=payload["limits"],
                )
            )
            return attach_active_session_cookie(response, job_id)
        except Exception as exc:  # noqa: BLE001
            flash(f"Erreur Calendrier : {exc}", "error")
            return redirect(url_for("index"))

    @app.get("/calendar/<job_id>/ical")
    def calendar_ical(job_id: str):
        if not re.fullmatch(r"[0-9a-f]{32}", job_id):
            return "Invalid job", 400
        job_path = JOB_DIR / job_id
        workbook_path = resolve_session_workbook(job_path)
        if workbook_path is None:
            return "Session expirée.", 410
        try:
            import datetime as dt_mod

            dataset = get_workbook_dataset(workbook_path)
            year = dataset.available_years[-1]
            chain_weekly = dataset.chain_weekly
            year_frame = chain_weekly[chain_weekly["annee_da"] == year]

            lines = [
                "BEGIN:VCALENDAR",
                "VERSION:2.0",
                "PRODID:-//HR Brasserie//Maintenance Calendar//FR",
                "CALSCALE:GREGORIAN",
                "METHOD:PUBLISH",
            ]
            for chain in sorted(year_frame["IDChaine"].unique()):
                chain_frame = year_frame[year_frame["IDChaine"] == chain]
                upper_avg = chain_frame["upper"].mean()
                if pd.isna(upper_avg):
                    continue
                for _, row in chain_frame.iterrows():
                    value = row["value"]
                    week = int(row["week_num"])
                    if pd.notna(value) and value > upper_avg:
                        try:
                            date_start = dt_mod.date.fromisocalendar(year, week, 1)
                            date_end = date_start + dt_mod.timedelta(days=7)
                            lines += [
                                "BEGIN:VEVENT",
                                f"UID:hrbrasserie-{year}-{chain}-W{week:02d}@hr-brasserie",
                                f"DTSTART;VALUE=DATE:{date_start.strftime('%Y%m%d')}",
                                f"DTEND;VALUE=DATE:{date_end.strftime('%Y%m%d')}",
                                f"SUMMARY:Arrêts élevés {chain} — Sem.{week:02d} ({value * 100:.1f}%)",
                                f"DESCRIPTION:Chaîne {chain} - Taux {value * 100:.2f}% > limite sup {upper_avg * 100:.2f}%",
                                "END:VEVENT",
                            ]
                        except ValueError:
                            pass
            lines.append("END:VCALENDAR")
            return Response(
                "\r\n".join(lines),
                mimetype="text/calendar",
                headers={"Content-Disposition": f"attachment; filename=maintenance_{year}.ics"},
            )
        except Exception as exc:  # noqa: BLE001
            return f"Erreur : {exc}", 500

    @app.post("/export-excel/<job_id>")
    def export_excel(job_id: str):
        if not re.fullmatch(r"[0-9a-f]{32}", job_id):
            return jsonify(error="Session invalide."), 400
        job_path = JOB_DIR / job_id
        workbook_path = resolve_session_workbook(job_path)
        if workbook_path is None:
            return jsonify(error="Session expirée."), 400
        try:
            dataset = get_workbook_dataset(workbook_path)
            year = dataset.available_years[-1]
            export_dir = job_path / "exports"
            export_dir.mkdir(parents=True, exist_ok=True)
            out_path = export_dir / f"dashboard_{year}.xlsx"
            build_excel_export(dataset, year, out_path)
            try:
                groups = dataset.available_groups(year, None, list(DEFAULT_FOCUS_NATURES))
                archive_report(
                    final_path=out_path,
                    filename=out_path.name,
                    year=year,
                    chains=sorted({group.chain for group in groups}),
                    natures=sorted({group.nature for group in groups}),
                    mode="excel",
                    source_workbook=resolve_session_workbook_name(job_path),
                    archive_dir=ARCHIVE_DIR,
                    batch_id=uuid.uuid4().hex,
                    artifact_kind="excel_dashboard",
                )
            except Exception:
                pass
            return send_file(out_path, as_attachment=True, download_name=f"dashboard_arrets_{year}.xlsx")
        except Exception as exc:  # noqa: BLE001
            return jsonify(error=str(exc)), 500

    @app.post("/api/test-smtp")
    def api_test_smtp():
        ok, message = test_smtp_connection(load_settings())
        if ok:
            return jsonify(ok=True, message=message)
        return jsonify(ok=False, error=message)

    @app.get("/settings")
    def settings_page():
        settings = load_settings()
        safe_settings = {key: value for key, value in settings.items() if key != "smtp_pass"}
        safe_settings["has_password"] = bool(settings.get("smtp_pass"))
        return render_template("settings.html", s=safe_settings)

    @app.post("/settings")
    def settings_save():
        data: dict[str, Any] = {
            "smtp_host": (request.form.get("smtp_host") or "").strip(),
            "smtp_port": int(request.form.get("smtp_port") or 587),
            "smtp_user": (request.form.get("smtp_user") or "").strip(),
            "smtp_from": (request.form.get("smtp_from") or "").strip(),
            "smtp_from_name": (request.form.get("smtp_from_name") or "RAPPORT PDF").strip(),
            "smtp_use_tls": request.form.get("smtp_use_tls") == "on",
            "smtp_use_ssl": request.form.get("smtp_use_ssl") == "on",
            "subject_template": (request.form.get("subject_template") or "").strip(),
            "email_signature": (request.form.get("email_signature") or "").strip(),
        }
        raw_recipients = request.form.get("default_recipients", "")
        data["default_recipients"] = [email.strip() for email in re.split(r"[;,\n]+", raw_recipients) if email.strip()]
        new_password = request.form.get("smtp_pass", "").strip()
        if new_password:
            data["smtp_pass"] = new_password
        try:
            save_settings(data)
            flash("Paramètres sauvegardés avec succès.", "success")
        except Exception as exc:  # noqa: BLE001
            flash(f"Erreur lors de la sauvegarde : {exc}", "error")
        return redirect(url_for("settings_page"))

    return app
