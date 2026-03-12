#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

MPL_CACHE_DIR = Path("/tmp/hr_brasserie_mpl_cache")
MPL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPL_CACHE_DIR))
os.environ.setdefault("XDG_CACHE_HOME", str(MPL_CACHE_DIR.parent))

import matplotlib

matplotlib.use("Agg")

import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import PercentFormatter


DEFAULT_FOCUS_NATURES = ("ELEC", "EXPL", "MECA", "SGX")
NUMERIC_COLUMNS = (
    "annee_da",
    "semaine_da",
    "DureeArret",
    "SUM_tu",
    "Duree_Sem_H",
    "semaine",
    "Duree_Sem_OK",
    "trp_test",
    "trg_test",
    "DureeArret_H",
    "Taux",
    "TRP_TRG.TRP",
    "TRP_TRG.TRG",
    "Conf_limites.Limite Inf",
    "Conf_limites.Limite Sup",
    "Conf_limites.Limite Inf TRP",
    "Conf_limites.Limite Sup TRP",
    "Conf_limites.Limite Inf TRG",
    "Conf_limites.Limite Sup TRG",
    "Taux_par_Chaine.Taux",
    "Taux_par_Chaine_Nature.Taux",
    "Cumul.Taux_cumul",
    "Taux_an_prec",
    "Taux_ch_nat_prec2025",
    "Taux_ch_nat_prec2024",
    "Taux_ch_prec2025",
    "Taux_ch_prec2024",
    "Limite Inf",
    "Limite Sup",
    "Limite Inf TRP",
    "Limite Sup TRP",
    "Limite Inf TRG",
    "Limite Sup TRG",
)
MACHINE_COLOR = "#0F4C81"
CHAIN_COLOR = "#2A9D8F"
NATURE_COLOR = "#C84B31"
CUMUL_COLOR = "#6A4C93"
PREVIOUS_YEAR_COLOR = "#7A7D85"
CURRENT_YEAR_AVG_COLOR = "#FFB000"
TRP_COLOR = "#118AB2"
TRG_COLOR = "#EF476F"
LOWER_LIMIT_COLOR = "#F4A261"
UPPER_LIMIT_COLOR = "#D62828"
TARGET_BAND_COLOR = "#D9F0D8"

PDF_FOOTER_COLOR = "#9CA3AF"
PDF_HEADER_BG = "#1E3A5F"


@dataclass(frozen=True)
class GroupKey:
    chain: str
    nature: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Genere automatiquement des rapports PDF a partir d'un classeur "
            "Excel de type ANALYSE ARRETS."
        )
    )
    parser.add_argument(
        "input",
        nargs="?",
        help="Classeur Excel a analyser. Si omis, le premier .xlsx du dossier courant est utilise.",
    )
    parser.add_argument(
        "--output-dir",
        default="exports",
        help="Dossier de sortie des PDF. Defaut: exports",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Annee cible. Par defaut, la derniere annee presente dans Base.",
    )
    parser.add_argument(
        "--mode",
        choices=("grouped", "combined", "both"),
        default="grouped",
        help="Sortie par groupe, complete, ou les deux. Defaut: grouped",
    )
    parser.add_argument(
        "--chains",
        nargs="*",
        help="Filtrer les chaines a exporter, ex: CH2 CH5",
    )
    parser.add_argument(
        "--natures",
        nargs="*",
        help="Filtrer les natures a exporter, ex: ELEC MECA",
    )
    parser.add_argument(
        "--equipments",
        nargs="*",
        help="Limiter les dashboards a certains equipements.",
    )
    parser.add_argument(
        "--skip-overview",
        action="store_true",
        help="Ne pas ajouter la page de synthese multi-machines au debut de chaque groupe.",
    )
    return parser.parse_args()


def find_default_workbook() -> Path:
    candidates = sorted(Path.cwd().glob("*.xlsx"))
    if not candidates:
        raise FileNotFoundError("Aucun fichier .xlsx trouve dans le dossier courant.")
    return candidates[0]


def normalize_text(series: pd.Series, uppercase: bool = False) -> pd.Series:
    values = (
        series.fillna("")
        .astype(str)
        .str.replace("\xa0", " ", regex=False)
        .str.strip()
    )
    values = values.mask(values.str.lower().eq("nan"), "")
    if uppercase:
        values = values.str.upper()
    return values


def normalize_numeric(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")

    values = normalize_text(series)
    values = values.str.replace(" ", "", regex=False)
    values = values.str.replace(",", ".", regex=False)
    values = values.replace("", np.nan)
    return pd.to_numeric(values, errors="coerce")


def extract_week_number(row: pd.Series) -> float:
    for column in ("semaine", "semaine_da"):
        value = row.get(column)
        if pd.notna(value):
            return float(value)

    for column in ("Sem_Text", "semaine_textuelle", "semaine_textuelle_da"):
        raw_value = str(row.get(column, "")).strip()
        match = re.search(r"(\d+)\s*$", raw_value)
        if match:
            return float(match.group(1))

    return np.nan


def first_valid(series: pd.Series) -> float | str | None:
    cleaned = series.dropna()
    if cleaned.empty:
        return None
    return cleaned.iloc[0]


def format_percent(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:.2%}"


def slugify(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", value.strip()).strip("_")


def ensure_theme() -> None:
    sns.set_theme(
        style="whitegrid",
        rc={
            "axes.facecolor": "#F8FBFF",
            "figure.facecolor": "#FFFFFF",
            "axes.edgecolor": "#C9CDD3",
            "grid.color": "#E8ECF2",
            "grid.linestyle": "-",
            "grid.alpha": 0.8,
            "axes.titlesize": 10,
            "axes.titleweight": "bold",
            "axes.titlecolor": "#1E3A5F",
            "axes.labelsize": 8,
            "legend.fontsize": 7.5,
            "legend.framealpha": 0.0,
            "xtick.labelsize": 7.5,
            "ytick.labelsize": 7.5,
            "axes.spines.top": False,
            "axes.spines.right": False,
        },
    )


class WorkbookDataset:
    def __init__(self, workbook_path: Path, focus_natures: Sequence[str]) -> None:
        self.workbook_path = workbook_path
        self.focus_natures = tuple(normalize_text(pd.Series(focus_natures), uppercase=True))
        self.base = self._load_base()
        self.limits = self._load_limits()

        available_years = (
            self.base["annee_da"]
            .dropna()
            .astype(int)
            .sort_values()
            .unique()
            .tolist()
        )
        if not available_years:
            raise ValueError("Aucune annee exploitable n'a ete trouvee dans la feuille Base.")
        self.available_years = available_years

        self.detail = self._prepare_detail_rows()
        self.focus_detail = self.detail[self.detail["Nature"].isin(self.focus_natures)].copy()
        if self.focus_detail.empty:
            raise ValueError(
                "Aucune donnee detaillee ne correspond aux natures cibles "
                f"{', '.join(self.focus_natures)}."
            )

        self.equipment_weekly = self._build_equipment_weekly()
        self.chain_weekly = self._build_chain_weekly()
        self.chain_nature_weekly = self._build_chain_nature_weekly()
        self.chain_nature_cumulative = self._build_chain_nature_cumulative()
        self.chain_trp = self._build_chain_metric(
            value_column="trp_test",
            low_column="Conf_limites.Limite Inf TRP",
            high_column="Conf_limites.Limite Sup TRP",
        )
        self.chain_trg = self._build_chain_metric(
            value_column="trg_test",
            low_column="Conf_limites.Limite Inf TRG",
            high_column="Conf_limites.Limite Sup TRG",
        )

        self.equipment_year_avg = self._build_year_average(
            self.equipment_weekly,
            keys=("annee_da", "IDChaine", "Nature", "Equipement"),
        )
        self.chain_year_avg = self._build_year_average(
            self.chain_weekly,
            keys=("annee_da", "IDChaine"),
        )
        self.chain_nature_year_avg = self._build_year_average(
            self.chain_nature_weekly,
            keys=("annee_da", "IDChaine", "Nature"),
        )

    def _load_base(self) -> pd.DataFrame:
        try:
            base = pd.read_excel(self.workbook_path, sheet_name="Base")
        except ValueError as exc:
            raise ValueError(
                "La feuille 'Base' est absente. Le script attend le meme type de classeur "
                "que celui analyse dans le projet."
            ) from exc

        base.columns = [str(column).strip() for column in base.columns]
        for column in ("IDChaine", "Nature", "Equipement", "Sem_Text", "semaine_textuelle_da"):
            if column in base.columns:
                base[column] = normalize_text(
                    base[column],
                    uppercase=column in {"IDChaine", "Nature", "Equipement"},
                )

        for column in NUMERIC_COLUMNS:
            if column in base.columns:
                base[column] = normalize_numeric(base[column])

        base["week_num"] = base.apply(extract_week_number, axis=1)
        base["week_num"] = pd.to_numeric(base["week_num"], errors="coerce")
        base["week_label"] = normalize_text(base.get("Sem_Text", pd.Series(dtype="object")))
        fallback_mask = base["week_label"].eq("")
        if "annee_da" in base.columns:
            year_values = base["annee_da"].fillna(0).astype(int)
            week_values = base["week_num"].fillna(0).astype(int)
            base.loc[fallback_mask, "week_label"] = (
                year_values.astype(str) + ",Sem." + week_values.map("{:02d}".format)
            )
        return base

    def _load_limits(self) -> pd.DataFrame:
        try:
            limits = pd.read_excel(self.workbook_path, sheet_name="Limites")
        except ValueError:
            return pd.DataFrame()

        limits.columns = [str(column).strip() for column in limits.columns]
        for column in ("Nature", "IDChaine", "Equipement"):
            if column in limits.columns:
                limits[column] = normalize_text(
                    limits[column],
                    uppercase=column in {"Nature", "IDChaine", "Equipement"},
                )
        for column in NUMERIC_COLUMNS:
            if column in limits.columns:
                limits[column] = normalize_numeric(limits[column])
        return limits

    def _prepare_detail_rows(self) -> pd.DataFrame:
        detail = self.base.copy()
        mask = (
            detail["annee_da"].notna()
            & detail["week_num"].notna()
            & detail["Equipement"].ne("")
            & detail["IDChaine"].ne("")
            & detail["Nature"].ne("")
            & detail["DureeArret"].notna()
            & detail["Taux"].notna()
        )
        detail = detail.loc[mask].copy()
        detail["annee_da"] = detail["annee_da"].astype(int)
        detail["week_num"] = detail["week_num"].astype(int)
        detail = detail.sort_values(
            ["annee_da", "IDChaine", "Nature", "Equipement", "week_num"],
            kind="stable",
        )
        return detail

    def _build_equipment_weekly(self) -> pd.DataFrame:
        aggregated = (
            self.focus_detail.groupby(
                ["annee_da", "IDChaine", "Nature", "Equipement", "week_num"],
                as_index=False,
            )
            .agg(
                week_label=("week_label", first_valid),
                value=("Taux", "mean"),
                lower=("Conf_limites.Limite Inf", "mean"),
                upper=("Conf_limites.Limite Sup", "mean"),
            )
            .sort_values(["annee_da", "IDChaine", "Nature", "Equipement", "week_num"])
        )
        return aggregated

    def _build_chain_weekly(self) -> pd.DataFrame:
        aggregated = (
            self.focus_detail.groupby(["annee_da", "IDChaine", "week_num"], as_index=False)
            .agg(
                week_label=("week_label", first_valid),
                value=("Taux", "mean"),
                lower=("Conf_limites.Limite Inf", "mean"),
                upper=("Conf_limites.Limite Sup", "mean"),
            )
            .sort_values(["annee_da", "IDChaine", "week_num"])
        )
        return aggregated

    def _build_chain_nature_weekly(self) -> pd.DataFrame:
        aggregated = (
            self.focus_detail.groupby(
                ["annee_da", "IDChaine", "Nature", "week_num"],
                as_index=False,
            )
            .agg(
                week_label=("week_label", first_valid),
                value=("Taux", "mean"),
                lower=("Conf_limites.Limite Inf", "mean"),
                upper=("Conf_limites.Limite Sup", "mean"),
            )
            .sort_values(["annee_da", "IDChaine", "Nature", "week_num"])
        )
        return aggregated

    def _build_chain_nature_cumulative(self) -> pd.DataFrame:
        cumulative = self.chain_nature_weekly.copy()
        cumulative["value"] = (
            cumulative.groupby(["annee_da", "IDChaine", "Nature"])["value"]
            .transform(lambda series: series.expanding().mean())
        )
        return cumulative

    def _build_chain_metric(
        self,
        value_column: str,
        low_column: str,
        high_column: str,
    ) -> pd.DataFrame:
        aggregated = (
            self.detail.groupby(["annee_da", "IDChaine", "week_num"], as_index=False)
            .agg(
                week_label=("week_label", first_valid),
                value=(value_column, "mean"),
                lower=(low_column, "mean"),
                upper=(high_column, "mean"),
            )
            .sort_values(["annee_da", "IDChaine", "week_num"])
        )
        return aggregated

    def _build_year_average(
        self,
        weekly_frame: pd.DataFrame,
        keys: Sequence[str],
    ) -> pd.DataFrame:
        return weekly_frame.groupby(list(keys), as_index=False).agg(avg_value=("value", "mean"))

    def previous_year(self, year: int) -> int | None:
        previous = [candidate for candidate in self.available_years if candidate < year]
        return previous[-1] if previous else None

    def available_groups(
        self,
        year: int,
        chains: Sequence[str] | None = None,
        natures: Sequence[str] | None = None,
    ) -> list[GroupKey]:
        frame = self.equipment_weekly[self.equipment_weekly["annee_da"] == year]
        if chains:
            frame = frame[frame["IDChaine"].isin(chains)]
        if natures:
            frame = frame[frame["Nature"].isin(natures)]

        groups = (
            frame[["IDChaine", "Nature"]]
            .drop_duplicates()
            .sort_values(["IDChaine", "Nature"], kind="stable")
        )
        return [GroupKey(row.IDChaine, row.Nature) for row in groups.itertuples(index=False)]

    def equipments_for_group(
        self,
        year: int,
        group: GroupKey,
        equipments: Sequence[str] | None = None,
    ) -> list[str]:
        frame = self.equipment_year_avg
        frame = frame[
            (frame["annee_da"] == year)
            & (frame["IDChaine"] == group.chain)
            & (frame["Nature"] == group.nature)
        ]
        if equipments:
            frame = frame[frame["Equipement"].isin(equipments)]
        frame = frame.sort_values(["avg_value", "Equipement"], ascending=[False, True])
        return frame["Equipement"].tolist()

    def series_for_equipment(self, year: int, group: GroupKey, equipment: str) -> pd.DataFrame:
        frame = self.equipment_weekly
        return frame[
            (frame["annee_da"] == year)
            & (frame["IDChaine"] == group.chain)
            & (frame["Nature"] == group.nature)
            & (frame["Equipement"] == equipment)
        ].sort_values("week_num")

    def series_for_group_total(self, year: int, group: GroupKey) -> pd.DataFrame:
        frame = self.chain_weekly
        return frame[
            (frame["annee_da"] == year)
            & (frame["IDChaine"] == group.chain)
        ].sort_values("week_num")

    def series_for_group_nature(self, year: int, group: GroupKey) -> pd.DataFrame:
        frame = self.chain_nature_weekly
        return frame[
            (frame["annee_da"] == year)
            & (frame["IDChaine"] == group.chain)
            & (frame["Nature"] == group.nature)
        ].sort_values("week_num")

    def cumulative_for_group_nature(self, year: int, group: GroupKey) -> pd.DataFrame:
        frame = self.chain_nature_cumulative
        return frame[
            (frame["annee_da"] == year)
            & (frame["IDChaine"] == group.chain)
            & (frame["Nature"] == group.nature)
        ].sort_values("week_num")

    def trp_for_chain(self, year: int, chain: str) -> pd.DataFrame:
        frame = self.chain_trp
        return frame[
            (frame["annee_da"] == year)
            & (frame["IDChaine"] == chain)
        ].sort_values("week_num")

    def trg_for_chain(self, year: int, chain: str) -> pd.DataFrame:
        frame = self.chain_trg
        return frame[
            (frame["annee_da"] == year)
            & (frame["IDChaine"] == chain)
        ].sort_values("week_num")

    def current_year_average_for_equipment(
        self,
        year: int,
        group: GroupKey,
        equipment: str,
    ) -> float | None:
        return self._lookup_year_average(
            self.equipment_year_avg,
            filters={
                "annee_da": year,
                "IDChaine": group.chain,
                "Nature": group.nature,
                "Equipement": equipment,
            },
        )

    def previous_year_average_for_equipment(
        self,
        year: int,
        group: GroupKey,
        equipment: str,
    ) -> float | None:
        previous = self.previous_year(year)
        if previous is None:
            return None
        return self.current_year_average_for_equipment(previous, group, equipment)

    def current_year_average_for_chain(self, year: int, chain: str) -> float | None:
        return self._lookup_year_average(
            self.chain_year_avg,
            filters={"annee_da": year, "IDChaine": chain},
        )

    def previous_year_average_for_chain(self, year: int, chain: str) -> float | None:
        previous = self.previous_year(year)
        if previous is None:
            return None
        return self.current_year_average_for_chain(previous, chain)

    def current_year_average_for_group(self, year: int, group: GroupKey) -> float | None:
        return self._lookup_year_average(
            self.chain_nature_year_avg,
            filters={"annee_da": year, "IDChaine": group.chain, "Nature": group.nature},
        )

    def previous_year_average_for_group(self, year: int, group: GroupKey) -> float | None:
        previous = self.previous_year(year)
        if previous is None:
            return None
        return self.current_year_average_for_group(previous, group)

    def equipment_matrix(self, year: int, group: GroupKey) -> pd.DataFrame:
        frame = self.equipment_weekly
        filtered = frame[
            (frame["annee_da"] == year)
            & (frame["IDChaine"] == group.chain)
            & (frame["Nature"] == group.nature)
        ]
        if filtered.empty:
            return pd.DataFrame()
        pivot = (
            filtered.pivot_table(
                index="week_num",
                columns="Equipement",
                values="value",
                aggfunc="mean",
            )
            .sort_index()
            .sort_index(axis=1)
        )
        return pivot

    def current_equipment_ranking(self, year: int, group: GroupKey) -> pd.DataFrame:
        frame = self.equipment_year_avg
        return frame[
            (frame["annee_da"] == year)
            & (frame["IDChaine"] == group.chain)
            & (frame["Nature"] == group.nature)
        ].sort_values("avg_value", ascending=False)

    def limit_for_equipment(self, group: GroupKey, equipment: str) -> tuple[float | None, float | None]:
        row = self._limit_row(group=group, equipment=equipment)
        if row is None:
            return (None, None)
        return (self._to_scalar(row.get("Limite Inf")), self._to_scalar(row.get("Limite Sup")))

    def limit_for_group(self, group: GroupKey) -> tuple[float | None, float | None]:
        row = self._limit_row(group=group)
        if row is None:
            return (None, None)
        return (self._to_scalar(row.get("Limite Inf")), self._to_scalar(row.get("Limite Sup")))

    def trp_limits_for_chain(self, chain: str) -> tuple[float | None, float | None]:
        row = self._limit_row(chain=chain)
        if row is None:
            return (None, None)
        return (
            self._to_scalar(row.get("Limite Inf TRP")),
            self._to_scalar(row.get("Limite Sup TRP")),
        )

    def trg_limits_for_chain(self, chain: str) -> tuple[float | None, float | None]:
        row = self._limit_row(chain=chain)
        if row is None:
            return (None, None)
        return (
            self._to_scalar(row.get("Limite Inf TRG")),
            self._to_scalar(row.get("Limite Sup TRG")),
        )

    def _lookup_year_average(self, frame: pd.DataFrame, filters: dict[str, object]) -> float | None:
        filtered = frame
        for column, value in filters.items():
            filtered = filtered[filtered[column] == value]
        if filtered.empty:
            return None
        return self._to_scalar(filtered["avg_value"].mean())

    def _limit_row(
        self,
        group: GroupKey | None = None,
        equipment: str | None = None,
        chain: str | None = None,
    ) -> pd.Series | None:
        if self.limits.empty:
            return None

        filtered = self.limits.copy()
        if group is not None:
            filtered = filtered[
                (filtered["IDChaine"] == group.chain) & (filtered["Nature"] == group.nature)
            ]
        elif chain is not None:
            filtered = filtered[filtered["IDChaine"] == chain]
            filtered = filtered[filtered["Nature"].isin(self.focus_natures)]

        if equipment is not None:
            filtered = filtered[filtered["Equipement"] == equipment]

        if filtered.empty:
            return None

        numeric = filtered.select_dtypes(include=["number"]).mean(numeric_only=True)
        return numeric

    @staticmethod
    def _to_scalar(value: object) -> float | None:
        if value is None or pd.isna(value):
            return None
        return float(value)


def configure_week_axis(ax: plt.Axes, weeks: Sequence[int]) -> None:
    if not weeks:
        return

    unique_weeks = sorted({int(week) for week in weeks})
    tick_step = 1
    if len(unique_weeks) > 12:
        tick_step = 2
    if len(unique_weeks) > 24:
        tick_step = 4
    if len(unique_weeks) > 40:
        tick_step = 6

    ticks = unique_weeks[::tick_step]
    ax.set_xlim(min(unique_weeks) - 0.75, max(unique_weeks) + 0.75)
    ax.set_xticks(ticks)
    ax.set_xticklabels([f"S{week:02d}" for week in ticks], rotation=0)
    ax.set_xlabel("Semaines")


def setup_metric_axis(ax: plt.Axes, title: str) -> None:
    ax.set_title(title, loc="left", pad=6, fontsize=10, fontweight="bold", color="#1E3A5F")
    ax.yaxis.set_major_formatter(PercentFormatter(1.0))
    ax.set_ylabel("Taux", fontsize=8, color="#4B5563")
    ax.grid(True, axis="y", linewidth=0.6, color="#E8ECF2")
    ax.grid(False, axis="x")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#D1D5DB")
    ax.spines["bottom"].set_color("#D1D5DB")
    ax.tick_params(axis="both", colors="#6B7280", length=3)


def add_pdf_footer(fig: plt.Figure, workbook_name: str, page_label: str) -> None:
    fig.text(
        0.5,
        0.005,
        f"{workbook_name}  —  {page_label}",
        ha="center",
        va="bottom",
        fontsize=6.5,
        color=PDF_FOOTER_COLOR,
        style="italic",
    )


def add_target_band(
    ax: plt.Axes,
    lower: float | None,
    upper: float | None,
    label_prefix: str = "",
) -> None:
    if lower is not None and upper is not None and upper >= lower:
        ax.axhspan(lower, upper, color=TARGET_BAND_COLOR, alpha=0.65, zorder=0)
        ax.axhline(
            lower,
            color=LOWER_LIMIT_COLOR,
            linewidth=1.25,
            linestyle="--",
            label=f"{label_prefix}Limite inf",
        )
        ax.axhline(
            upper,
            color=UPPER_LIMIT_COLOR,
            linewidth=1.25,
            linestyle="--",
            label=f"{label_prefix}Limite sup",
        )
        return

    if lower is not None:
        ax.axhline(
            lower,
            color=LOWER_LIMIT_COLOR,
            linewidth=1.25,
            linestyle="--",
            label=f"{label_prefix}Limite inf",
        )
    if upper is not None:
        ax.axhline(
            upper,
            color=UPPER_LIMIT_COLOR,
            linewidth=1.25,
            linestyle="--",
            label=f"{label_prefix}Limite sup",
        )


def add_constant_line(
    ax: plt.Axes,
    value: float | None,
    label: str,
    color: str,
    linestyle: str,
) -> None:
    if value is None or pd.isna(value):
        return
    ax.axhline(value, color=color, linewidth=1.6, linestyle=linestyle, label=label)


def choose_bar_colors(values: Sequence[float], lower: float | None, upper: float | None) -> list[str]:
    colors: list[str] = []
    for value in values:
        if lower is not None and value < lower:
            colors.append(MACHINE_COLOR)
        elif upper is not None and value > upper:
            colors.append(UPPER_LIMIT_COLOR)
        else:
            colors.append(CURRENT_YEAR_AVG_COLOR)
    return colors


def draw_empty_panel(ax: plt.Axes, title: str, message: str = "Aucune donnee disponible") -> None:
    setup_metric_axis(ax, title)
    ax.text(0.5, 0.5, message, transform=ax.transAxes, ha="center", va="center", fontsize=11)
    ax.set_xticks([])
    ax.set_yticks([])


def plot_bar_metric(
    ax: plt.Axes,
    frame: pd.DataFrame,
    title: str,
    bar_label: str,
    bar_color: str,
    lower: float | None,
    upper: float | None,
    previous_year_avg: float | None,
    current_year_avg: float | None,
) -> None:
    if frame.empty:
        draw_empty_panel(ax, title)
        return

    setup_metric_axis(ax, title)
    add_target_band(ax, lower, upper)

    weeks = frame["week_num"].astype(int).tolist()
    values = frame["value"].tolist()
    colors = choose_bar_colors(values, lower, upper) if bar_color == CURRENT_YEAR_AVG_COLOR else [bar_color] * len(values)

    ax.bar(weeks, values, width=0.72, color=colors, edgecolor="white", linewidth=0.8, label=bar_label)
    ax.plot(weeks, values, color="#1F2937", linewidth=1.2, marker="o", markersize=3, alpha=0.7)
    add_constant_line(ax, previous_year_avg, "Moyenne annee precedente", PREVIOUS_YEAR_COLOR, "--")
    add_constant_line(ax, current_year_avg, "Moyenne annee courante", CURRENT_YEAR_AVG_COLOR, "-.")
    configure_week_axis(ax, weeks)
    ax.legend(loc="upper right", frameon=False)


def plot_line_metric(
    ax: plt.Axes,
    frame: pd.DataFrame,
    title: str,
    series_label: str,
    series_color: str,
    lower: float | None,
    upper: float | None,
    previous_year_avg: float | None = None,
    current_year_avg: float | None = None,
) -> None:
    if frame.empty:
        draw_empty_panel(ax, title)
        return

    setup_metric_axis(ax, title)
    add_target_band(ax, lower, upper)

    weeks = frame["week_num"].astype(int).tolist()
    values = frame["value"].tolist()
    ax.plot(
        weeks,
        values,
        color=series_color,
        linewidth=2.2,
        marker="o",
        markersize=4,
        label=series_label,
    )
    add_constant_line(ax, previous_year_avg, "Moyenne annee precedente", PREVIOUS_YEAR_COLOR, "--")
    add_constant_line(ax, current_year_avg, "Moyenne annee courante", CURRENT_YEAR_AVG_COLOR, "-.")
    configure_week_axis(ax, weeks)
    ax.legend(loc="upper right", frameon=False)


def plot_overview_page(
    dataset: WorkbookDataset,
    year: int,
    group: GroupKey,
) -> plt.Figure:
    previous_year = dataset.previous_year(year)
    matrix = dataset.equipment_matrix(year, group)
    group_series = dataset.series_for_group_nature(year, group)
    cumulative_series = dataset.cumulative_for_group_nature(year, group)
    ranking = dataset.current_equipment_ranking(year, group).head(12)
    lower, upper = dataset.limit_for_group(group)

    fig = plt.figure(figsize=(16, 9))
    grid = fig.add_gridspec(2, 2, height_ratios=[2.2, 1.25], hspace=0.35, wspace=0.2)
    ax_main = fig.add_subplot(grid[0, :])
    ax_summary = fig.add_subplot(grid[1, 0])
    ax_cumulative = fig.add_subplot(grid[1, 1])

    fig.suptitle(
        f"Synthese groupe {group.chain} / {group.nature} - {year}",
        x=0.02,
        y=0.98,
        ha="left",
        fontsize=18,
        fontweight="bold",
    )
    fig.text(
        0.02,
        0.94,
        (
            f"Fichier: {dataset.workbook_path.name} | "
            f"Machines: {matrix.shape[1] if not matrix.empty else 0} | "
            f"Semaines: {matrix.shape[0] if not matrix.empty else 0} | "
            f"Moyenne {year}: {format_percent(dataset.current_year_average_for_group(year, group))}"
            + (
                f" | Moyenne {previous_year}: {format_percent(dataset.previous_year_average_for_group(year, group))}"
                if previous_year is not None
                else ""
            )
        ),
        ha="left",
        va="top",
        fontsize=9,
        color="#4B5563",
    )

    if matrix.empty:
        draw_empty_panel(ax_main, "Courbes par equipement")
    else:
        setup_metric_axis(ax_main, "Courbes hebdomadaires par equipement")
        add_target_band(ax_main, lower, upper)
        palette = sns.color_palette("tab20", n_colors=max(matrix.shape[1], 3))
        for index, equipment in enumerate(matrix.columns):
            ax_main.plot(
                matrix.index,
                matrix[equipment],
                linewidth=1.5,
                marker="o",
                markersize=2.8,
                alpha=0.9,
                color=palette[index % len(palette)],
                label=equipment,
            )
        if not group_series.empty:
            ax_main.plot(
                group_series["week_num"],
                group_series["value"],
                linewidth=3.0,
                color="#111827",
                label="Moyenne groupe",
            )
        add_constant_line(
            ax_main,
            dataset.previous_year_average_for_group(year, group),
            "Moyenne annee precedente",
            PREVIOUS_YEAR_COLOR,
            "--",
        )
        configure_week_axis(ax_main, matrix.index.tolist())
        ax_main.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), frameon=False, ncol=1)

    plot_bar_metric(
        ax=ax_summary,
        frame=group_series,
        title="Moyenne du groupe",
        bar_label=f"Taux moyen {group.chain} / {group.nature}",
        bar_color=CHAIN_COLOR,
        lower=lower,
        upper=upper,
        previous_year_avg=dataset.previous_year_average_for_group(year, group),
        current_year_avg=dataset.current_year_average_for_group(year, group),
    )

    plot_line_metric(
        ax=ax_cumulative,
        frame=cumulative_series,
        title="Taux cumulatif du groupe",
        series_label="Cumul glissant",
        series_color=CUMUL_COLOR,
        lower=None,
        upper=None,
        previous_year_avg=dataset.previous_year_average_for_group(year, group),
        current_year_avg=dataset.current_year_average_for_group(year, group),
    )

    if not ranking.empty:
        summary_lines = [
            f"{row.Equipement}: {format_percent(row.avg_value)}"
            for row in ranking.itertuples(index=False)
        ]
        fig.text(
            0.815,
            0.175,
            "Top machines:\n" + "\n".join(summary_lines),
            ha="left",
            va="top",
            fontsize=7.5,
            color="#374151",
            bbox={
                "boxstyle": "round,pad=0.5",
                "facecolor": "#F9FAFB",
                "edgecolor": "#D1D5DB",
                "linewidth": 0.8,
            },
        )

    add_pdf_footer(
        fig,
        dataset.workbook_path.name,
        f"Synthese {group.chain} / {group.nature} — {year}",
    )
    return fig


def plot_dashboard_page(
    dataset: WorkbookDataset,
    year: int,
    group: GroupKey,
    equipment: str,
) -> plt.Figure:
    previous_year = dataset.previous_year(year)

    equipment_series = dataset.series_for_equipment(year, group, equipment)
    total_series = dataset.series_for_group_total(year, group)
    trp_series = dataset.trp_for_chain(year, group.chain)
    trg_series = dataset.trg_for_chain(year, group.chain)
    group_series = dataset.series_for_group_nature(year, group)
    cumulative_series = dataset.cumulative_for_group_nature(year, group)

    equipment_lower = dataset.limit_for_equipment(group, equipment)[0]
    equipment_upper = dataset.limit_for_equipment(group, equipment)[1]
    group_lower, group_upper = dataset.limit_for_group(group)
    trp_lower, trp_upper = dataset.trp_limits_for_chain(group.chain)
    trg_lower, trg_upper = dataset.trg_limits_for_chain(group.chain)

    fig = plt.figure(figsize=(16, 9))
    grid = fig.add_gridspec(3, 2, hspace=0.35, wspace=0.2)
    ax_machine = fig.add_subplot(grid[0, 0])
    ax_total = fig.add_subplot(grid[0, 1])
    ax_trp = fig.add_subplot(grid[1, 0])
    ax_group = fig.add_subplot(grid[1, 1])
    ax_trg = fig.add_subplot(grid[2, 0])
    ax_cumulative = fig.add_subplot(grid[2, 1])

    fig.suptitle(
        f"{group.chain} / {group.nature} / {equipment} - {year}",
        x=0.02,
        y=0.985,
        ha="left",
        fontsize=18,
        fontweight="bold",
    )
    fig.text(
        0.02,
        0.95,
        (
            f"Fichier: {dataset.workbook_path.name} | "
            f"Moyenne machine {year}: {format_percent(dataset.current_year_average_for_equipment(year, group, equipment))}"
            + (
                f" | Moyenne machine {previous_year}: "
                f"{format_percent(dataset.previous_year_average_for_equipment(year, group, equipment))}"
                if previous_year is not None
                else ""
            )
        ),
        ha="left",
        va="top",
        fontsize=9,
        color="#4B5563",
    )

    plot_bar_metric(
        ax=ax_machine,
        frame=equipment_series,
        title=f"Taux machine - {equipment}",
        bar_label=f"Taux {year}",
        bar_color=CURRENT_YEAR_AVG_COLOR,
        lower=equipment_lower,
        upper=equipment_upper,
        previous_year_avg=dataset.previous_year_average_for_equipment(year, group, equipment),
        current_year_avg=dataset.current_year_average_for_equipment(year, group, equipment),
    )

    plot_bar_metric(
        ax=ax_total,
        frame=total_series,
        title=f"Taux moyen total chaine - {group.chain}",
        bar_label=f"Taux moyen {group.chain}",
        bar_color=CHAIN_COLOR,
        lower=group_lower,
        upper=group_upper,
        previous_year_avg=dataset.previous_year_average_for_chain(year, group.chain),
        current_year_avg=dataset.current_year_average_for_chain(year, group.chain),
    )

    plot_line_metric(
        ax=ax_trp,
        frame=trp_series,
        title=f"TRP chaine - {group.chain}",
        series_label="TRP",
        series_color=TRP_COLOR,
        lower=trp_lower,
        upper=trp_upper,
    )

    plot_bar_metric(
        ax=ax_group,
        frame=group_series,
        title=f"Taux moyen {group.chain} / {group.nature}",
        bar_label=f"Taux moyen {group.nature}",
        bar_color=NATURE_COLOR,
        lower=group_lower,
        upper=group_upper,
        previous_year_avg=dataset.previous_year_average_for_group(year, group),
        current_year_avg=dataset.current_year_average_for_group(year, group),
    )

    plot_line_metric(
        ax=ax_trg,
        frame=trg_series,
        title=f"TRG chaine - {group.chain}",
        series_label="TRG",
        series_color=TRG_COLOR,
        lower=trg_lower,
        upper=trg_upper,
    )

    plot_line_metric(
        ax=ax_cumulative,
        frame=cumulative_series,
        title=f"Taux cumulatif {group.chain} / {group.nature}",
        series_label="Cumul glissant",
        series_color=CUMUL_COLOR,
        lower=None,
        upper=None,
        previous_year_avg=dataset.previous_year_average_for_group(year, group),
        current_year_avg=dataset.current_year_average_for_group(year, group),
    )

    add_pdf_footer(
        fig,
        dataset.workbook_path.name,
        f"{group.chain} / {group.nature} / {equipment} — {year}",
    )
    return fig


def save_group_pdf(
    pdf_path: Path,
    dataset: WorkbookDataset,
    year: int,
    group: GroupKey,
    equipments: Sequence[str],
    include_overview: bool,
) -> None:
    with PdfPages(pdf_path) as pdf:
        if include_overview:
            overview = plot_overview_page(dataset, year, group)
            pdf.savefig(overview, bbox_inches="tight")
            plt.close(overview)

        for equipment in equipments:
            figure = plot_dashboard_page(dataset, year, group, equipment)
            pdf.savefig(figure, bbox_inches="tight")
            plt.close(figure)

    print(f"PDF genere: {pdf_path}")


ProgressCallback = Callable[[int, str], None]


def generate_reports(
    dataset: WorkbookDataset,
    output_dir: Path,
    year: int,
    mode: str,
    groups: Sequence[GroupKey],
    equipments_filter: Sequence[str] | None,
    include_overview: bool,
    grouped_subdirs: bool = False,
    progress_cb: ProgressCallback | None = None,
) -> list[Path]:
    def _cb(pct: int, msg: str) -> None:
        if progress_cb is not None:
            try:
                progress_cb(pct, msg)
            except Exception:
                pass

    generated_files: list[Path] = []
    output_dir.mkdir(parents=True, exist_ok=True)

    combined_path = output_dir / f"rapport_arrets_{year}_complet.pdf"
    combined_pdf: PdfPages | None = None

    total = len(groups)
    _cb(10, "Préparation du rapport…")

    if mode in {"combined", "both"}:
        combined_pdf = PdfPages(combined_path)

    try:
        for index, group in enumerate(groups, start=1):
            equipments = dataset.equipments_for_group(year, group, equipments_filter)
            if not equipments:
                continue

            # Progression linéaire : 10 % → 88 % sur l'ensemble des groupes
            pct = 10 + int(78 * (index - 1) / max(total, 1))
            _cb(
                pct,
                f"Groupe {index}/{total} — {group.chain} / {group.nature} "
                f"({len(equipments)} équipement{'s' if len(equipments) > 1 else ''})",
            )

            print(
                f"[{index}/{total}] Generation du groupe {group.chain} / {group.nature} "
                f"({len(equipments)} equipements)"
            )

            if mode in {"grouped", "both"}:
                group_pdf_dir = output_dir
                if grouped_subdirs:
                    group_pdf_dir = output_dir / slugify(group.chain) / slugify(group.nature)
                    group_pdf_dir.mkdir(parents=True, exist_ok=True)
                group_pdf_path = group_pdf_dir / f"{slugify(group.chain)}_{slugify(group.nature)}_{year}.pdf"
                save_group_pdf(
                    pdf_path=group_pdf_path,
                    dataset=dataset,
                    year=year,
                    group=group,
                    equipments=equipments,
                    include_overview=include_overview,
                )
                generated_files.append(group_pdf_path)

            if combined_pdf is not None:
                if include_overview:
                    overview = plot_overview_page(dataset, year, group)
                    combined_pdf.savefig(overview, bbox_inches="tight")
                    plt.close(overview)
                for equipment in equipments:
                    figure = plot_dashboard_page(dataset, year, group, equipment)
                    combined_pdf.savefig(figure, bbox_inches="tight")
                    plt.close(figure)

        _cb(92, "Assemblage du document final…")
        if combined_pdf is not None:
            generated_files.append(combined_path)
    finally:
        if combined_pdf is not None:
            combined_pdf.close()

    _cb(98, "Finalisation…")
    return generated_files


def main() -> None:
    ensure_theme()
    args = parse_args()

    workbook_path = Path(args.input) if args.input else find_default_workbook()
    if not workbook_path.exists():
        raise FileNotFoundError(f"Classeur introuvable: {workbook_path}")

    requested_natures = args.natures if args.natures else DEFAULT_FOCUS_NATURES
    requested_natures = normalize_text(pd.Series(requested_natures), uppercase=True).tolist()
    chains = normalize_text(pd.Series(args.chains), uppercase=True).tolist() if args.chains else None
    equipments = (
        normalize_text(pd.Series(args.equipments), uppercase=True).tolist()
        if args.equipments
        else None
    )

    dataset = WorkbookDataset(workbook_path=workbook_path, focus_natures=DEFAULT_FOCUS_NATURES)
    year = args.year or dataset.available_years[-1]
    if year not in dataset.available_years:
        raise ValueError(
            f"Annee {year} absente du classeur. Annees disponibles: {dataset.available_years}"
        )

    groups = dataset.available_groups(year=year, chains=chains, natures=requested_natures)
    if not groups:
        raise ValueError("Aucun groupe a exporter avec les filtres fournis.")

    generated = generate_reports(
        dataset=dataset,
        output_dir=Path(args.output_dir),
        year=year,
        mode=args.mode,
        groups=groups,
        equipments_filter=equipments,
        include_overview=not args.skip_overview,
    )

    if not generated:
        raise RuntimeError("Aucun PDF n'a ete genere.")

    print("\nGeneration terminee. Fichiers produits:")
    for path in generated:
        print(f"- {path}")


if __name__ == "__main__":
    main()
