from __future__ import annotations

import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

from .logging_setup import get_logger

logger = get_logger("mail")


def send_pdf_email(
    recipients: list[str],
    pdf_path: Path,
    subject: str,
    body: str,
    settings: dict[str, Any],
) -> None:
    from_name = settings.get("smtp_from_name", "RAPPORT PDF")
    from_addr = settings["smtp_from"]
    from_field = f"{from_name} <{from_addr}>" if from_name else from_addr

    message = MIMEMultipart()
    message["From"] = from_field
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain", "utf-8"))

    with pdf_path.open("rb") as handle:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(handle.read())
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{pdf_path.name}"')
    message.attach(part)

    host = settings["smtp_host"]
    port = int(settings["smtp_port"])

    if settings.get("smtp_use_ssl"):
        server = smtplib.SMTP_SSL(host, port, timeout=20)
    else:
        server = smtplib.SMTP(host, port, timeout=20)
        if settings.get("smtp_use_tls"):
            server.starttls()

    try:
        if settings.get("smtp_user") and settings.get("smtp_pass"):
            server.login(settings["smtp_user"], settings["smtp_pass"])
        server.sendmail(from_addr, recipients, message.as_bytes())
        logger.info("Email envoyé à %s avec pièce jointe %s", ", ".join(recipients), pdf_path.name)
    finally:
        server.quit()


def test_smtp_connection(settings: dict[str, Any]) -> tuple[bool, str]:
    try:
        host = settings["smtp_host"]
        port = int(settings["smtp_port"])
        if settings.get("smtp_use_ssl"):
            server = smtplib.SMTP_SSL(host, port, timeout=10)
        else:
            server = smtplib.SMTP(host, port, timeout=10)
            if settings.get("smtp_use_tls"):
                server.ehlo()
                server.starttls()
                server.ehlo()
        if settings.get("smtp_user") and settings.get("smtp_pass"):
            server.login(settings["smtp_user"], settings["smtp_pass"])
        server.quit()
        logger.info("Test SMTP réussi sur %s:%s", settings["smtp_host"], settings["smtp_port"])
        return True, "Connexion SMTP réussie ✓"
    except smtplib.SMTPAuthenticationError:
        logger.warning("Échec d'authentification SMTP sur %s", settings.get("smtp_host"))
        return False, "Identifiant ou mot de passe SMTP incorrect."
    except OSError as exc:
        logger.exception("Connexion SMTP impossible")
        return False, f"Impossible de joindre {settings.get('smtp_host')} : {exc}"
    except Exception as exc:  # noqa: BLE001
        logger.exception("Erreur SMTP inattendue")
        return False, str(exc)
