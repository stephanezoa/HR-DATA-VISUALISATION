from __future__ import annotations

import re
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd

from generate_arrets_reports import WorkbookDataset, normalize_text, slugify

from .config import APP_XML_NS, CORE_XML_NS


def normalize_checkbox_list(values: list[str]) -> list[str]:
    if not values:
        return []
    return normalize_text(pd.Series(values), uppercase=True).tolist()


def parse_equipment_filters(raw: str) -> list[str]:
    if not raw.strip():
        return []
    values = [value for value in re.split(r"[,;\n]+", raw) if value.strip()]
    return normalize_text(pd.Series(values), uppercase=True).tolist()


def build_export_label(chains: list[str], natures: list[str], mode: str) -> str:
    parts: list[str] = []
    if chains:
        parts.append("_".join(chains[:3]) + ("_etc" if len(chains) > 3 else ""))
    if natures:
        parts.append("_".join(natures))
    parts.append(mode)
    return "_".join(parts) or mode


def selection_slug(values: list[str], prefix: str, fallback: str, limit: int = 4) -> str:
    cleaned = [slugify(value).lower() for value in values if str(value).strip()]
    if not cleaned:
        return fallback
    excerpt = cleaned[:limit]
    if len(cleaned) > limit:
        excerpt.append("etc")
    return f"{prefix}-{'-'.join(excerpt)}"


def scope_slug(chains: list[str], natures: list[str]) -> str:
    return "__".join(
        [
            selection_slug(chains, "chaines", "toutes-chaines"),
            selection_slug(natures, "categories", "toutes-categories"),
        ]
    )


def thematic_group_pdf_name(year: int, chain: str, nature: str) -> str:
    return (
        f"rapport_arrets_{year}_chaine-{slugify(chain).lower()}"
        f"_categorie-{slugify(nature).lower()}_groupe.pdf"
    )


def thematic_combined_pdf_name(year: int, chains: list[str], natures: list[str]) -> str:
    return f"rapport_arrets_{year}_{scope_slug(chains, natures)}_complet.pdf"


def thematic_bundle_name(
    year: int,
    chains: list[str],
    natures: list[str],
    mode: str,
    variant: str,
) -> str:
    variant_label = {
        "standard": "selection",
        "by_chain": "packs-par-chaine",
        "by_nature": "packs-par-categorie",
        "by_group": "packs-par-groupe",
    }.get(variant, slugify(variant))
    mode_label = {
        "combined": "pdf-complet",
        "grouped": "pdf-par-groupe",
        "both": "mixte",
    }.get(mode, slugify(mode))
    return f"pack_arrets_{year}_{scope_slug(chains, natures)}_{variant_label}_{mode_label}.zip"


def read_excel_metadata(workbook_path: Path) -> dict[str, Any]:
    try:
        with zipfile.ZipFile(workbook_path) as archive:
            app_root = ET.fromstring(archive.read("docProps/app.xml"))
            core_root = ET.fromstring(archive.read("docProps/core.xml"))
            sheets = [
                node.text or ""
                for node in app_root.findall(".//app:TitlesOfParts//{*}lpstr", APP_XML_NS)
                if (node.text or "").strip()
            ]
            return {
                "application": app_root.findtext("app:Application", default="", namespaces=APP_XML_NS),
                "excel_version": app_root.findtext("app:AppVersion", default="", namespaces=APP_XML_NS),
                "creator": core_root.findtext("dc:creator", default="", namespaces=CORE_XML_NS),
                "modified": core_root.findtext("dcterms:modified", default="", namespaces=CORE_XML_NS),
                "sheets": sheets,
            }
    except (KeyError, ET.ParseError):
        return {
            "application": "Microsoft Excel",
            "excel_version": "",
            "creator": "",
            "modified": "",
            "sheets": [],
        }


def build_download_bundle(
    zip_path: Path,
    output_dir: Path,
    workbook_name: str,
    metadata: dict[str, Any],
    year: int,
    mode: str,
) -> None:
    lines = [
        f"Fichier source : {workbook_name}",
        f"Application    : {metadata.get('application') or 'Microsoft Excel'}",
        f"Auteur         : {metadata.get('creator') or 'inconnu'}",
        f"Modification   : {metadata.get('modified') or 'inconnue'}",
        f"Année exportée : {year}",
        f"Mode export    : {mode}",
        "",
        "Feuilles détectées :",
    ]
    lines.extend(f"  - {sheet}" for sheet in metadata.get("sheets", []))
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("metadata.txt", "\n".join(lines))
        for pdf in sorted(output_dir.rglob("*.pdf")):
            relative = pdf.relative_to(output_dir)
            if relative.parent == Path(".") and pdf.name.endswith("_complet.pdf"):
                target = Path("combined") / pdf.name
            elif relative.parent == Path("."):
                target = Path("grouped") / pdf.name
            else:
                target = Path("grouped") / relative
            archive.write(pdf, target.as_posix())


def build_excel_export(dataset: WorkbookDataset, year: int, out_path: Path) -> None:
    import xlsxwriter

    equipment_weekly = dataset.equipment_weekly
    equipment_year = equipment_weekly[equipment_weekly["annee_da"] == year].copy()
    chain_weekly = dataset.chain_weekly
    chain_year = chain_weekly[chain_weekly["annee_da"] == year].copy()

    workbook = xlsxwriter.Workbook(str(out_path))

    title_fmt = workbook.add_format({"bold": True, "font_size": 14, "font_color": "#1E3A5F"})
    header_fmt = workbook.add_format({
        "bold": True,
        "font_color": "#FFFFFF",
        "bg_color": "#1E3A5F",
        "border": 1,
        "border_color": "#FFFFFF",
        "align": "center",
    })
    data_fmt = workbook.add_format({"border": 1, "border_color": "#E0E0E0"})
    pct_fmt = workbook.add_format({"border": 1, "border_color": "#E0E0E0", "num_format": "0.00%"})
    alt_fmt = workbook.add_format({"bg_color": "#F8FBFF", "border": 1, "border_color": "#E0E0E0"})
    alt_pct = workbook.add_format({"bg_color": "#F8FBFF", "border": 1, "border_color": "#E0E0E0", "num_format": "0.00%"})

    sheet = workbook.add_worksheet("Synthèse équipements")
    sheet.write(0, 0, f"Tableau de bord — Arrêts Production {year}", title_fmt)
    headers = ["Chaîne", "Nature", "Équipement", f"Taux moyen {year}", "Limite inf", "Limite sup"]
    for col, header in enumerate(headers):
        sheet.write(2, col, header, header_fmt)

    equipment_avg = (
        equipment_year.groupby(["IDChaine", "Nature", "Equipement"])
        .agg(value=("value", "mean"), lower=("lower", "mean"), upper=("upper", "mean"))
        .reset_index()
        .sort_values(["IDChaine", "Nature", "value"], ascending=[True, True, False])
        .reset_index(drop=True)
    )
    for index, row in equipment_avg.iterrows():
        alt = index % 2 == 0
        sheet.write(index + 3, 0, row["IDChaine"], alt_fmt if alt else data_fmt)
        sheet.write(index + 3, 1, row["Nature"], alt_fmt if alt else data_fmt)
        sheet.write(index + 3, 2, row["Equipement"], alt_fmt if alt else data_fmt)
        sheet.write(index + 3, 3, round(float(row["value"]), 4) if pd.notna(row["value"]) else "", alt_pct if alt else pct_fmt)
        sheet.write(index + 3, 4, round(float(row["lower"]), 4) if pd.notna(row["lower"]) else "", alt_pct if alt else pct_fmt)
        sheet.write(index + 3, 5, round(float(row["upper"]), 4) if pd.notna(row["upper"]) else "", alt_pct if alt else pct_fmt)

    sheet.set_column(0, 0, 12)
    sheet.set_column(1, 1, 10)
    sheet.set_column(2, 2, 26)
    sheet.set_column(3, 5, 16)

    weekly_sheet = workbook.add_worksheet("Chaînes hebdo")
    chains = sorted(chain_year["IDChaine"].unique())
    weeks = sorted(chain_year["week_num"].dropna().astype(int).unique())

    weekly_sheet.write(0, 0, f"Taux moyen par chaîne — Hebdomadaire {year}", title_fmt)
    weekly_sheet.write(2, 0, "Semaine", header_fmt)
    for index, chain in enumerate(chains):
        weekly_sheet.write(2, index + 1, chain, header_fmt)
    for row_index, week in enumerate(weeks):
        weekly_sheet.write(row_index + 3, 0, int(week), alt_fmt if row_index % 2 == 0 else data_fmt)
        for col_index, chain in enumerate(chains):
            mask = (chain_year["IDChaine"] == chain) & (chain_year["week_num"].astype(int) == week)
            values = chain_year.loc[mask, "value"]
            fmt = alt_pct if row_index % 2 == 0 else pct_fmt
            weekly_sheet.write(
                row_index + 3,
                col_index + 1,
                round(float(values.iloc[0]), 4) if not values.empty else "",
                fmt,
            )

    weekly_sheet.set_column(0, 0, 10)
    for index in range(len(chains)):
        weekly_sheet.set_column(index + 1, index + 1, 12)

    chart_data = workbook.add_worksheet("_chart_data")
    chart_data.hide()
    chain_avgs = chain_year.groupby("IDChaine")["value"].mean().reset_index()
    for index, row in chain_avgs.iterrows():
        chart_data.write(index, 0, row["IDChaine"])
        chart_data.write(index, 1, round(float(row["value"]), 4))

    chart = workbook.add_chart({"type": "bar"})
    chart.add_series({
        "categories": f"='_chart_data'!$A$1:$A${len(chain_avgs)}",
        "values": f"='_chart_data'!$B$1:$B${len(chain_avgs)}",
        "name": f"Taux moyen {year}",
        "fill": {"color": "#0F4C81"},
    })
    chart.set_title({"name": f"Taux moyen par chaîne — {year}"})
    chart.set_y_axis({"num_format": "0%"})
    chart.set_size({"width": 480, "height": 300})
    weekly_sheet.insert_chart(3, len(chains) + 2, chart)

    workbook.close()
