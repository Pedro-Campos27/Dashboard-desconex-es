from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


SCRIPT_DIR = Path(__file__).resolve().parent
AFTER_DIR = SCRIPT_DIR / "saida_ubidots_analise"
BEFORE_ROOT = SCRIPT_DIR / "antes"
PLOTLY_TEMPLATE = "plotly_white"
CRITICAL_HOURS = {20, 21, 22, 23, 0, 1, 2, 3, 4, 5, 6, 7}

DATE_COLUMNS = {
    "dia",
    "inicio_periodo",
    "fim_periodo",
    "inicio_desconexao",
    "fim_desconexao",
    "datahora",
}

NUMERIC_COLUMNS = {
    "timestamp",
    "valor",
    "total_minutos_esperados",
    "minutos_com_dado",
    "minutos_sem_dado",
    "percentual_conectado",
    "percentual_desconectado",
    "quantidade_desconexoes_gt_20min",
    "menor_desconexao_min",
    "maior_desconexao_min",
    "media_desconexao_min",
    "tempo_total_desconectado_min",
    "desconexoes_no_periodo_critico_20h_08h",
    "duracao_min",
}

BOOL_MAPPINGS = {
    "true": True,
    "false": False,
    "1": True,
    "0": False,
    "sim": True,
    "nao": False,
}

FILE_MAP = {
    "metricas_periodo": "metricas_periodo.csv",
    "metricas_por_dia": "metricas_por_dia.csv",
    "eventos": "eventos_desconexao.csv",
    "dados_normalizados": "dados_normalizados_1min.csv",
    "dados_brutos": "dados_brutos.csv",
}


def fmt_num(value: float | int | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_int(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{int(round(value)):,}".replace(",", ".")


def fmt_duration(minutes: float) -> str:
    """Formata minutos como '2h 23min' ou '45min'."""
    if pd.isna(minutes) or minutes < 0:
        return "-"
    total = int(round(minutes))
    hours, mins = divmod(total, 60)
    if hours > 0 and mins > 0:
        return f"{hours}h {mins}min"
    if hours > 0:
        return f"{hours}h"
    return f"{mins}min"


def hour_label(hour: int) -> str:
    return f"{hour:02d}:00"


def build_period_label_from_days(days: list[pd.Timestamp]) -> str:
    if not days:
        return "Periodo indisponivel"
    start = pd.Timestamp(days[0])
    end = pd.Timestamp(days[-1])
    return f"{start.strftime('%d/%m/%Y')} a {end.strftime('%d/%m/%Y')}"


def read_csv_flex(path: Path) -> pd.DataFrame:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return pd.read_csv(path, encoding=encoding)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path)


def normalize_frame(df: pd.DataFrame, sensor: str | None = None) -> pd.DataFrame:
    df = df.copy()
    df.columns = [column.strip() for column in df.columns]
    df = df.rename(
        columns={
            "duracao_minutos": "duracao_min",
            "periodo_critico_20h_08h": "periodo_critico",
        }
    )

    if "variavel" not in df.columns and sensor:
        df["variavel"] = sensor

    if "sensor" not in df.columns:
        if "variavel" in df.columns:
            df["sensor"] = df["variavel"]
            if sensor is not None:
                df["sensor"] = df["sensor"].fillna(sensor)
        else:
            df["sensor"] = sensor

    for column in DATE_COLUMNS.intersection(df.columns):
        df[column] = pd.to_datetime(df[column], dayfirst=True, errors="coerce")

    for column in NUMERIC_COLUMNS.intersection(df.columns):
        df[column] = pd.to_numeric(df[column], errors="coerce")

    if "periodo_critico" in df.columns:
        df["periodo_critico"] = (
            df["periodo_critico"]
            .astype(str)
            .str.strip()
            .str.lower()
            .map(BOOL_MAPPINGS)
            .fillna(False)
        )

    return df


def maybe_read_csv(path: Path, sensor: str | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return normalize_frame(read_csv_flex(path), sensor=sensor)


def list_before_periods(root: Path) -> list[Path]:
    if not root.exists():
        return []

    return sorted(
        [
            path
            for path in root.iterdir()
            if path.is_dir() and (path / "resumo_geral.csv").exists()
        ]
    )


def _load_dataset(base_dir_str: str, scenario: str) -> dict[str, object]:
    base_dir = Path(base_dir_str)
    if not base_dir.exists():
        raise FileNotFoundError(f"Pasta nao encontrada: {base_dir}")

    data: dict[str, list[pd.DataFrame]] = {name: [] for name in FILE_MAP}
    inventory_rows: list[dict[str, object]] = []

    resumo_geral = maybe_read_csv(base_dir / "resumo_geral.csv")
    if not resumo_geral.empty:
        resumo_geral["cenario"] = scenario
        inventory_rows.append(
            {
                "cenario": scenario,
                "sensor": "geral",
                "tipo": "resumo_geral",
                "linhas": len(resumo_geral),
                "arquivo": str((base_dir / "resumo_geral.csv").resolve()),
            }
        )

    for sensor_dir in sorted(path for path in base_dir.iterdir() if path.is_dir()):
        sensor = sensor_dir.name
        for key, filename in FILE_MAP.items():
            file_path = sensor_dir / filename
            frame = maybe_read_csv(file_path, sensor=sensor)
            if not frame.empty:
                frame["cenario"] = scenario
            if file_path.exists():
                inventory_rows.append(
                    {
                        "cenario": scenario,
                        "sensor": sensor,
                        "tipo": key,
                        "linhas": len(frame),
                        "arquivo": str(file_path.resolve()),
                    }
                )
                data[key].append(frame)

    combined = {
        key: pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        for key, frames in data.items()
    }

    available_days = []
    if not combined["metricas_por_dia"].empty and "dia" in combined["metricas_por_dia"].columns:
        available_days = sorted(combined["metricas_por_dia"]["dia"].dropna().dt.normalize().unique().tolist())

    return {
        "cenario": scenario,
        "base_dir": str(base_dir.resolve()),
        "period_label": build_period_label_from_days(available_days),
        "available_days": available_days,
        "inventory": pd.DataFrame(inventory_rows),
        "resumo_geral": resumo_geral,
        **combined,
    }


load_dataset = (
    st.cache_data(show_spinner=False)(_load_dataset)
    if st.runtime.exists()
    else _load_dataset
)


def filter_date_range(
    df: pd.DataFrame,
    column: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    if df.empty or column not in df.columns:
        return df.copy()

    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return df.loc[df[column].between(start, end)].copy()


def filter_events_range(
    df: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    if {"inicio_desconexao", "fim_desconexao"}.issubset(df.columns):
        mask = (df["inicio_desconexao"] <= end) & (df["fim_desconexao"] >= start)
        return df.loc[mask].copy()

    if "fim_desconexao" in df.columns:
        return df.loc[df["fim_desconexao"].between(start, end)].copy()

    return df.copy()


def filter_by_sensors(df: pd.DataFrame, sensors: list[str]) -> pd.DataFrame:
    if df.empty or "sensor" not in df.columns:
        return df.copy()
    return df.loc[df["sensor"].isin(sensors)].copy()


def count_sample_days(metricas_por_dia: pd.DataFrame) -> int:
    if metricas_por_dia.empty or "dia" not in metricas_por_dia.columns:
        return 0
    return int(metricas_por_dia["dia"].dropna().dt.normalize().nunique())


def filter_dataset(
    dataset: dict[str, object],
    sensors: list[str],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> dict[str, object]:
    inventory = dataset["inventory"]
    if isinstance(inventory, pd.DataFrame) and not inventory.empty and "sensor" in inventory.columns:
        inventory = inventory.loc[
            inventory["sensor"].isin(sensors) | (inventory["sensor"] == "geral")
        ].copy()

    filtered = {
        "cenario": dataset["cenario"],
        "base_dir": dataset["base_dir"],
        "inventory": inventory,
    }

    resumo_geral = dataset["resumo_geral"]
    if isinstance(resumo_geral, pd.DataFrame) and not resumo_geral.empty:
        filtered["resumo_geral"] = filter_by_sensors(resumo_geral, sensors)
    else:
        filtered["resumo_geral"] = pd.DataFrame()

    filtered["metricas_periodo"] = filter_by_sensors(dataset["metricas_periodo"], sensors)
    filtered["metricas_por_dia"] = filter_date_range(
        filter_by_sensors(dataset["metricas_por_dia"], sensors),
        "dia",
        start_date,
        end_date,
    )
    filtered["eventos"] = filter_events_range(
        filter_by_sensors(dataset["eventos"], sensors),
        start_date,
        end_date,
    )
    filtered["dados_normalizados"] = filter_date_range(
        filter_by_sensors(dataset["dados_normalizados"], sensors),
        "datahora",
        start_date,
        end_date,
    )
    filtered["dados_brutos"] = filter_date_range(
        filter_by_sensors(dataset["dados_brutos"], sensors),
        "datahora",
        start_date,
        end_date,
    )

    filtered["sample_days"] = count_sample_days(filtered["metricas_por_dia"])
    filtered["period_label"] = (
        f"{pd.Timestamp(start_date).strftime('%d/%m/%Y')} a "
        f"{pd.Timestamp(end_date).strftime('%d/%m/%Y')}"
    )
    return filtered


def build_window_options(days: list[pd.Timestamp], window_days: int) -> list[dict[str, object]]:
    if not days:
        return []

    normalized_days = [pd.Timestamp(day).normalize() for day in days]
    unique_days = sorted(pd.Series(normalized_days).drop_duplicates().tolist())
    requested_days = max(1, int(window_days))

    if requested_days >= len(unique_days):
        start = unique_days[0]
        end = unique_days[-1]
        return [
            {
                "label": f"{start.strftime('%d/%m/%Y')} a {end.strftime('%d/%m/%Y')} ({len(unique_days)} dias)",
                "start": start,
                "end": end,
                "days": len(unique_days),
            }
        ]

    windows = []
    for index in range(len(unique_days) - requested_days + 1):
        start = unique_days[index]
        end = unique_days[index + requested_days - 1]
        windows.append(
            {
                "label": f"{start.strftime('%d/%m/%Y')} a {end.strftime('%d/%m/%Y')} ({requested_days} dias)",
                "start": start,
                "end": end,
                "days": requested_days,
            }
        )

    return windows


def stable_default_index(key: str, total_options: int) -> int:
    if total_options <= 0:
        return 0
    return sum(ord(char) for char in key) % total_options


def build_sensor_summary(
    metricas_por_dia: pd.DataFrame,
    eventos: pd.DataFrame,
    dados_normalizados: pd.DataFrame,
    dados_brutos: pd.DataFrame,
    scenario: str,
) -> pd.DataFrame:
    if metricas_por_dia.empty:
        return pd.DataFrame()

    resumo = (
        metricas_por_dia.groupby("sensor", as_index=False)
        .agg(
            dias_amostrados=("dia", lambda series: series.dropna().dt.normalize().nunique()),
            total_minutos_esperados=("total_minutos_esperados", "sum"),
            minutos_com_dado=("minutos_com_dado", "sum"),
            minutos_sem_dado=("minutos_sem_dado", "sum"),
        )
        .sort_values("sensor")
    )

    resumo["percentual_conectado"] = (
        resumo["minutos_com_dado"] / resumo["total_minutos_esperados"] * 100
    ).round(2)
    resumo["percentual_desconectado"] = (
        resumo["minutos_sem_dado"] / resumo["total_minutos_esperados"] * 100
    ).round(2)

    if not eventos.empty and "duracao_min" in eventos.columns:
        agregacoes_eventos = {
            "desconexoes_gt_20min": ("duracao_min", "size"),
            "tempo_total_desconectado_min": ("duracao_min", "sum"),
            "maior_evento_min": ("duracao_min", "max"),
            "media_evento_min": ("duracao_min", "mean"),
        }
        if "periodo_critico" in eventos.columns:
            agregacoes_eventos["desconexoes_criticas"] = ("periodo_critico", "sum")

        eventos_resumo = eventos.groupby("sensor", as_index=False).agg(**agregacoes_eventos)
        if "desconexoes_criticas" not in eventos_resumo.columns:
            eventos_resumo["desconexoes_criticas"] = 0
        resumo = resumo.merge(eventos_resumo, on="sensor", how="left")
    else:
        fallback_eventos = metricas_por_dia.groupby("sensor", as_index=False).agg(
            desconexoes_gt_20min=("quantidade_desconexoes_gt_20min", "sum"),
            tempo_total_desconectado_min=("tempo_total_desconectado_min", "sum"),
            desconexoes_criticas=("desconexoes_no_periodo_critico_20h_08h", "sum"),
        )
        resumo = resumo.merge(fallback_eventos, on="sensor", how="left")

    if not dados_normalizados.empty and "valor" in dados_normalizados.columns:
        temperaturas = dados_normalizados.groupby("sensor", as_index=False).agg(
            temperatura_media=("valor", "mean"),
            temperatura_min=("valor", "min"),
            temperatura_max=("valor", "max"),
        )
        resumo = resumo.merge(temperaturas, on="sensor", how="left")

    if not dados_brutos.empty:
        registros_brutos = (
            dados_brutos.groupby("sensor", as_index=False)
            .size()
            .rename(columns={"size": "registros_brutos"})
        )
        resumo = resumo.merge(registros_brutos, on="sensor", how="left")

    for column in [
        "desconexoes_gt_20min",
        "tempo_total_desconectado_min",
        "desconexoes_criticas",
    ]:
        if column in resumo.columns:
            resumo[column] = resumo[column].fillna(0)

    resumo["min_sem_dado_por_dia"] = resumo["minutos_sem_dado"] / resumo["dias_amostrados"]
    resumo["desconexoes_por_dia"] = resumo["desconexoes_gt_20min"] / resumo["dias_amostrados"]
    resumo["tempo_desconectado_por_dia"] = (
        resumo["tempo_total_desconectado_min"] / resumo["dias_amostrados"]
    )
    resumo["cenario"] = scenario

    return resumo.sort_values(
        ["minutos_sem_dado", "desconexoes_gt_20min"], ascending=[False, False]
    ).reset_index(drop=True)


def build_hourly_profile(dados_normalizados: pd.DataFrame, scenario: str) -> pd.DataFrame:
    if dados_normalizados.empty or "datahora" not in dados_normalizados.columns:
        return pd.DataFrame()

    frame = dados_normalizados.copy()
    frame = frame.dropna(subset=["datahora"])
    if frame.empty:
        return pd.DataFrame()

    frame["dia_ref"] = frame["datahora"].dt.normalize()
    frame["hora"] = frame["datahora"].dt.hour
    frame["com_dado"] = frame["valor"].notna().astype(int)

    profile = frame.groupby(["sensor", "hora"], as_index=False).agg(
        dias_amostrados=("dia_ref", "nunique"),
        minutos_esperados=("com_dado", "size"),
        minutos_com_dado=("com_dado", "sum"),
    )

    profile["minutos_sem_dado"] = profile["minutos_esperados"] - profile["minutos_com_dado"]
    profile["pct_conectado"] = (
        profile["minutos_com_dado"] / profile["minutos_esperados"] * 100
    ).round(2)
    profile["pct_sem_dado"] = (
        profile["minutos_sem_dado"] / profile["minutos_esperados"] * 100
    ).round(2)
    profile["min_sem_dado_por_dia"] = profile["minutos_sem_dado"] / profile["dias_amostrados"]
    profile["hora_label"] = profile["hora"].map(hour_label)
    profile["faixa"] = profile["hora"].apply(
        lambda hour: "Desconexões entre 20h e 08h" if hour in CRITICAL_HOURS else "Desconexões entre 08h e 20h"
    )
    profile["cenario"] = scenario
    return profile


def build_overall_hourly_profile(dados_normalizados: pd.DataFrame, scenario: str) -> pd.DataFrame:
    if dados_normalizados.empty or "datahora" not in dados_normalizados.columns:
        return pd.DataFrame()

    frame = dados_normalizados.copy()
    frame = frame.dropna(subset=["datahora"])
    if frame.empty:
        return pd.DataFrame()

    frame["dia_ref"] = frame["datahora"].dt.normalize()
    frame["hora"] = frame["datahora"].dt.hour
    frame["com_dado"] = frame["valor"].notna().astype(int)

    profile = frame.groupby("hora", as_index=False).agg(
        dias_amostrados=("dia_ref", "nunique"),
        minutos_esperados=("com_dado", "size"),
        minutos_com_dado=("com_dado", "sum"),
    )

    profile["minutos_sem_dado"] = profile["minutos_esperados"] - profile["minutos_com_dado"]
    profile["pct_conectado"] = (
        profile["minutos_com_dado"] / profile["minutos_esperados"] * 100
    ).round(2)
    profile["pct_sem_dado"] = (
        profile["minutos_sem_dado"] / profile["minutos_esperados"] * 100
    ).round(2)
    profile["min_sem_dado_por_dia"] = profile["minutos_sem_dado"] / profile["dias_amostrados"]
    profile["hora_label"] = profile["hora"].map(hour_label)
    profile["faixa"] = profile["hora"].apply(
        lambda hour: "Desconexões entre 20h e 08h" if hour in CRITICAL_HOURS else "Desconexões entre 08h e 20h"
    )
    profile["cenario"] = scenario
    return profile


def build_band_summary(hourly_profile: pd.DataFrame) -> pd.DataFrame:
    if hourly_profile.empty:
        return pd.DataFrame()

    summary = hourly_profile.groupby(["cenario", "faixa"], as_index=False).agg(
        minutos_esperados=("minutos_esperados", "sum"),
        minutos_sem_dado=("minutos_sem_dado", "sum"),
    )
    summary["pct_sem_dado"] = (
        summary["minutos_sem_dado"] / summary["minutos_esperados"] * 100
    ).round(2)
    return summary


def build_critical_pct_by_sensor(hourly_profile: pd.DataFrame, column_name: str) -> pd.DataFrame:
    if hourly_profile.empty:
        return pd.DataFrame(columns=["sensor", column_name])

    critical = hourly_profile.loc[hourly_profile["hora"].isin(CRITICAL_HOURS)].copy()
    if critical.empty:
        return pd.DataFrame(columns=["sensor", column_name])

    summary = critical.groupby("sensor", as_index=False).agg(
        minutos_esperados=("minutos_esperados", "sum"),
        minutos_sem_dado=("minutos_sem_dado", "sum"),
    )
    summary[column_name] = (summary["minutos_sem_dado"] / summary["minutos_esperados"] * 100).round(2)
    return summary[["sensor", column_name]]


def build_overall_metrics(
    sensor_summary: pd.DataFrame,
    hourly_profile: pd.DataFrame,
    sample_days: int,
) -> dict[str, float]:
    num_sensors = len(sensor_summary) if not sensor_summary.empty else 1
    if sensor_summary.empty or sample_days <= 0:
        return {
            "sample_days": float(sample_days),
            "num_sensors": num_sensors,
            "pct_conectado": 0.0,
            "min_sem_dado_por_dia": 0.0,
            "min_sem_dado_por_dia_por_sensor": 0.0,
            "desconexoes_por_dia": 0.0,
            "tempo_desconectado_por_dia": 0.0,
            "pct_sem_dado_critico": 0.0,
            "critical_min_sem_dado_por_dia": 0.0,
        }

    total_esperado = sensor_summary["total_minutos_esperados"].sum()
    total_com_dado = sensor_summary["minutos_com_dado"].sum()
    total_sem_dado = sensor_summary["minutos_sem_dado"].sum()
    total_eventos = sensor_summary["desconexoes_gt_20min"].sum()
    total_tempo_desconectado = sensor_summary["tempo_total_desconectado_min"].sum()

    critical_profile = hourly_profile.loc[hourly_profile["hora"].isin(CRITICAL_HOURS)].copy()
    critical_pct = 0.0
    critical_min_total = 0.0
    if not critical_profile.empty and critical_profile["minutos_esperados"].sum() > 0:
        critical_pct = (
            critical_profile["minutos_sem_dado"].sum() / critical_profile["minutos_esperados"].sum() * 100
        )
        critical_min_total = critical_profile["minutos_sem_dado"].sum()

    min_sem_dado_total_dia = total_sem_dado / sample_days

    return {
        "sample_days": float(sample_days),
        "num_sensors": num_sensors,
        "pct_conectado": round(total_com_dado / total_esperado * 100, 2) if total_esperado else 0.0,
        "min_sem_dado_por_dia": round(min_sem_dado_total_dia, 2),
        "min_sem_dado_por_dia_por_sensor": round(min_sem_dado_total_dia / num_sensors, 2),
        "desconexoes_por_dia": round(total_eventos / sample_days, 2),
        "tempo_desconectado_por_dia": round(total_tempo_desconectado / sample_days, 2),
        "pct_sem_dado_critico": round(critical_pct, 2),
        "critical_min_sem_dado_por_dia": round(critical_min_total / sample_days, 2),
    }


def build_sensor_comparison(
    before_summary: pd.DataFrame,
    after_summary: pd.DataFrame,
    before_hourly: pd.DataFrame,
    after_hourly: pd.DataFrame,
) -> pd.DataFrame:
    before_base = before_summary[
        [
            "sensor",
            "percentual_conectado",
            "min_sem_dado_por_dia",
            "desconexoes_por_dia",
            "tempo_desconectado_por_dia",
            "maior_evento_min",
        ]
    ].rename(
        columns={
            "percentual_conectado": "antes_pct_conectado",
            "min_sem_dado_por_dia": "antes_min_sem_dado_dia",
            "desconexoes_por_dia": "antes_desconexoes_dia",
            "tempo_desconectado_por_dia": "antes_tempo_desconectado_dia",
            "maior_evento_min": "antes_maior_evento_min",
        }
    )

    after_base = after_summary[
        [
            "sensor",
            "percentual_conectado",
            "min_sem_dado_por_dia",
            "desconexoes_por_dia",
            "tempo_desconectado_por_dia",
            "maior_evento_min",
        ]
    ].rename(
        columns={
            "percentual_conectado": "depois_pct_conectado",
            "min_sem_dado_por_dia": "depois_min_sem_dado_dia",
            "desconexoes_por_dia": "depois_desconexoes_dia",
            "tempo_desconectado_por_dia": "depois_tempo_desconectado_dia",
            "maior_evento_min": "depois_maior_evento_min",
        }
    )

    comparison = before_base.merge(after_base, on="sensor", how="outer")
    comparison = comparison.merge(
        build_critical_pct_by_sensor(before_hourly, "antes_pct_sem_dado_critico"),
        on="sensor",
        how="left",
    )
    comparison = comparison.merge(
        build_critical_pct_by_sensor(after_hourly, "depois_pct_sem_dado_critico"),
        on="sensor",
        how="left",
    )

    numeric_columns = [column for column in comparison.columns if column != "sensor"]
    for column in numeric_columns:
        comparison[column] = comparison[column].fillna(0)

    comparison["delta_pct_conectado_pp"] = (
        comparison["depois_pct_conectado"] - comparison["antes_pct_conectado"]
    ).round(2)
    comparison["delta_min_sem_dado_dia"] = (
        comparison["depois_min_sem_dado_dia"] - comparison["antes_min_sem_dado_dia"]
    ).round(2)
    comparison["delta_desconexoes_dia"] = (
        comparison["depois_desconexoes_dia"] - comparison["antes_desconexoes_dia"]
    ).round(2)
    comparison["delta_pct_sem_dado_critico_pp"] = (
        comparison["depois_pct_sem_dado_critico"] - comparison["antes_pct_sem_dado_critico"]
    ).round(2)
    comparison["reducao_min_sem_dado_dia"] = (
        comparison["antes_min_sem_dado_dia"] - comparison["depois_min_sem_dado_dia"]
    ).round(2)

    return comparison.sort_values("reducao_min_sem_dado_dia", ascending=False).reset_index(drop=True)


def prepare_sensor_comparison_display(comparison: pd.DataFrame) -> pd.DataFrame:
    if comparison.empty:
        return comparison

    display = comparison.copy()
    for column in display.columns:
        if column != "sensor":
            display[column] = display[column].round(2)
    return display


def build_event_hour_profile(events: pd.DataFrame, day_counts: dict[str, int]) -> pd.DataFrame:
    if events.empty or "inicio_desconexao" not in events.columns:
        return pd.DataFrame()

    profile = events.copy()
    profile["hora_inicio"] = profile["inicio_desconexao"].dt.hour
    profile["hora_label"] = profile["hora_inicio"].map(hour_label)

    summary = profile.groupby(["cenario", "hora_inicio", "hora_label"], as_index=False).agg(
        eventos=("duracao_min", "size"),
        duracao_total_min=("duracao_min", "sum"),
    )
    summary["dias_amostrados"] = summary["cenario"].map(day_counts).fillna(0)
    summary["eventos_por_dia"] = summary["eventos"] / summary["dias_amostrados"].replace(0, pd.NA)
    summary["duracao_total_por_dia"] = (
        summary["duracao_total_min"] / summary["dias_amostrados"].replace(0, pd.NA)
    )
    return summary.fillna(0)


def update_hour_axis(figure) -> None:
    figure.update_xaxes(
        tickmode="array",
        tickvals=list(range(24)),
        ticktext=[hour_label(hour) for hour in range(24)],
    )


def render_dashboard() -> None:
    st.set_page_config(page_title="Comparativo antes x depois", layout="wide")

    before_periods = list_before_periods(BEFORE_ROOT)
    if not before_periods:
        st.error(f"Nenhuma pasta comparavel encontrada em {BEFORE_ROOT}.")
        st.stop()

    try:
        after_dataset = load_dataset(str(AFTER_DIR), "Depois")
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.stop()

    before_options = {path.name: path for path in before_periods}
    before_labels = list(before_options.keys())

    st.title("Comparação antes x depois das desconexões")
    st.caption(
        "Os períodos são diferentes. Por isso, os gráficos principais usam hora do dia "
        "e métricas normalizadas por dia amostrado."
    )

    with st.sidebar:
        st.header("Comparação")
        selected_before_label = st.selectbox(
            "Pacote do antes",
            options=before_labels,
            index=len(before_labels) - 1,
        )

    before_dataset = load_dataset(str(before_options[selected_before_label]), "Antes")

    after_available_days = after_dataset["available_days"]
    if not after_available_days:
        st.error("O pacote do depois nao possui dias disponiveis.")
        st.stop()

    after_min_day = pd.Timestamp(after_available_days[0]).date()
    after_max_day = pd.Timestamp(after_available_days[-1]).date()

    with st.sidebar:
        after_interval = st.date_input(
            "Janela do depois",
            value=(after_min_day, after_max_day),
            min_value=after_min_day,
            max_value=after_max_day,
        )

    if isinstance(after_interval, tuple) and len(after_interval) == 2:
        after_start_date, after_end_date = after_interval
    else:
        after_start_date = after_end_date = after_interval

    after_metricas_all = filter_date_range(
        after_dataset["metricas_por_dia"],
        "dia",
        pd.Timestamp(after_start_date),
        pd.Timestamp(after_end_date),
    )
    after_sample_days = count_sample_days(after_metricas_all)
    if after_sample_days <= 0:
        st.warning("Nao ha dados no depois para a janela escolhida.")
        st.stop()

    before_window_options = build_window_options(before_dataset["available_days"], after_sample_days)
    default_before_window = stable_default_index(
        f"{selected_before_label}-{after_sample_days}", len(before_window_options)
    )

    with st.sidebar:
        before_window_index = st.selectbox(
            "Janela comparavel do antes",
            options=list(range(len(before_window_options))),
            index=default_before_window,
            format_func=lambda index: before_window_options[index]["label"],
        )

    before_window = before_window_options[before_window_index]
    before_start = before_window["start"]
    before_end = before_window["end"]
    after_start = pd.Timestamp(after_start_date)
    after_end = pd.Timestamp(after_end_date)

    before_metricas_all = filter_date_range(
        before_dataset["metricas_por_dia"],
        "dia",
        before_start,
        before_end,
    )

    common_sensors = sorted(
        set(before_metricas_all["sensor"].dropna().unique().tolist())
        & set(after_metricas_all["sensor"].dropna().unique().tolist())
    )
    if not common_sensors:
        st.error("Nao ha sensores em comum entre o antes e o depois no filtro atual.")
        st.stop()

    with st.sidebar:
        sensors = st.multiselect("Sensores", options=common_sensors, default=common_sensors)

    if not sensors:
        st.warning("Selecione ao menos um sensor.")
        st.stop()

    before_filtered = filter_dataset(before_dataset, sensors, before_start, before_end)
    after_filtered = filter_dataset(after_dataset, sensors, after_start, after_end)

    before_summary = build_sensor_summary(
        before_filtered["metricas_por_dia"],
        before_filtered["eventos"],
        before_filtered["dados_normalizados"],
        before_filtered["dados_brutos"],
        "Antes",
    )
    after_summary = build_sensor_summary(
        after_filtered["metricas_por_dia"],
        after_filtered["eventos"],
        after_filtered["dados_normalizados"],
        after_filtered["dados_brutos"],
        "Depois",
    )

    if before_summary.empty or after_summary.empty:
        st.warning("Nao foi possivel montar a comparacao com os filtros atuais.")
        st.stop()

    before_hourly = build_hourly_profile(before_filtered["dados_normalizados"], "Antes")
    after_hourly = build_hourly_profile(after_filtered["dados_normalizados"], "Depois")
    overall_hourly = pd.concat(
        [
            build_overall_hourly_profile(before_filtered["dados_normalizados"], "Antes"),
            build_overall_hourly_profile(after_filtered["dados_normalizados"], "Depois"),
        ],
        ignore_index=True,
    )
    hourly_long = pd.concat([before_hourly, after_hourly], ignore_index=True)
    summary_long = pd.concat([before_summary, after_summary], ignore_index=True)
    band_summary = build_band_summary(hourly_long)
    sensor_comparison = build_sensor_comparison(
        before_summary,
        after_summary,
        before_hourly,
        after_hourly,
    )

    before_metrics = build_overall_metrics(
        before_summary,
        before_hourly,
        before_filtered["sample_days"],
    )
    after_metrics = build_overall_metrics(
        after_summary,
        after_hourly,
        after_filtered["sample_days"],
    )

    event_day_counts = {
        "Antes": before_filtered["sample_days"],
        "Depois": after_filtered["sample_days"],
    }
    events_long = pd.concat(
        [
            before_filtered["eventos"].assign(cenario="Antes"),
            after_filtered["eventos"].assign(cenario="Depois"),
        ],
        ignore_index=True,
    )
    event_hour_profile = build_event_hour_profile(events_long, event_day_counts)

    before_info, after_info = st.columns(2)
    before_info.markdown(
        f"**Antes**  \n"
        f"Janela: {before_filtered['period_label']}  \n"
        f"Dias amostrados: {fmt_int(before_filtered['sample_days'])}  \n"
        
    )
    after_info.markdown(
        f"**Depois**  \n"
        f"Janela: {after_filtered['period_label']}  \n"
        f"Dias amostrados: {fmt_int(after_filtered['sample_days'])}  \n"
        
    )

    if before_metrics["desconexoes_por_dia"] > 0 and after_metrics["desconexoes_por_dia"] == 0:
        st.success(
            "No recorte atual, o depois ficou sem desconexoes > 20 min, "
            f"enquanto o antes teve {fmt_num(before_metrics['desconexoes_por_dia'])} por dia."
        )

    # ── KPI Cards ──
    def _kpi_card(
        title: str,
        before_val: float,
        after_val: float,
        unit: str,
        icon: str = "📊",
        invert: bool = False,
        delta_suffix: str = "",
        subtitle: str = "",
    ) -> None:
        delta = after_val - before_val
        abs_delta = abs(delta)

        if unit == "%":
            after_fmt = f"{fmt_num(after_val)}%"
            before_fmt = f"{fmt_num(before_val)}%"
            delta_text = f"{abs_delta:.1f} p.p."
        elif unit == "duration":
            after_fmt = fmt_duration(after_val)
            before_fmt = fmt_duration(before_val)
            delta_text = fmt_duration(abs_delta)
        else:
            after_fmt = fmt_num(after_val)
            before_fmt = fmt_num(before_val)
            delta_text = fmt_num(abs_delta)

        if delta_suffix:
            delta_text += delta_suffix

        improved = delta > 0
        if invert:
            improved = not improved

        if abs(delta) < 0.01:
            badge_color = "#6c757d"
            badge_bg = "#f0f0f0"
            badge_text = "sem alteração"
        elif improved:
            badge_color = "#1a7f37"
            badge_bg = "#dafbe1"
            badge_arrow = "↑ " if not invert else "↓ "
            badge_text = f"{badge_arrow}{delta_text} de melhora"
        else:
            badge_color = "#cf222e"
            badge_bg = "#ffebe9"
            badge_arrow = "↑ " if delta > 0 else "↓ "
            badge_text = f"{badge_arrow}{delta_text} (piora)"

        icon_bg = badge_bg
        subtitle_html = f'<div style="font-size:0.72em;color:#aaa;margin-top:2px;">{subtitle}</div>' if subtitle else "<!-- -->"

        st.markdown(
            f"""
            <div style="background:#fff;border:1px solid #e8e8e8;border-radius:12px;padding:18px 16px;">
                <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;">
                    <span style="background:{icon_bg};border-radius:8px;padding:4px 7px;font-size:1.1em;">{icon}</span>
                    <span style="font-size:0.82em;color:#666;font-weight:500;">{title}</span>
                </div>
                <div style="font-size:1.8em;font-weight:700;color:#111;line-height:1.1;">{after_fmt}</div>
                <div style="font-size:0.78em;color:#999;margin-top:4px;">era {before_fmt} antes</div>
                {subtitle_html}
                <div style="margin-top:10px;">
                    <span style="background:{badge_bg};color:{badge_color};font-size:0.73em;font-weight:600;padding:3px 8px;border-radius:6px;">
                        {badge_text}
                    </span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    kpi_1, kpi_2, kpi_3, kpi_4 = st.columns(4)
    with kpi_1:
        _kpi_card(
            "Conectividade geral",
            before_metrics["pct_conectado"],
            after_metrics["pct_conectado"],
            "%",
            icon="📶",
        )
    with kpi_2:
        _kpi_card(
            "Tempo offline por sensor (dia)",
            before_metrics["min_sem_dado_por_dia_por_sensor"],
            after_metrics["min_sem_dado_por_dia_por_sensor"],
            "duration",
            icon="⏱️",
            invert=True,
            delta_suffix="/dia",
        )
    with kpi_3:
        _kpi_card(
            "Quedas longas por dia",
            before_metrics["desconexoes_por_dia"],
            after_metrics["desconexoes_por_dia"],
            "",
            icon="🔌",
            invert=True,
            delta_suffix=" quedas/dia",
        )
    with kpi_4:
        after_crit_dur = fmt_duration(after_metrics["critical_min_sem_dado_por_dia"])
        before_crit_dur = fmt_duration(before_metrics["critical_min_sem_dado_por_dia"])
        _kpi_card(
            "Offline entre 20h e 8h",
            before_metrics["pct_sem_dado_critico"],
            after_metrics["pct_sem_dado_critico"],
            "%",
            icon="🌙",
            invert=True,
            
        )

    tab_geral, tab_sensor, tab_eventos = st.tabs(
        ["Visão geral", "Por sensor", "Eventos"]
    )

    with tab_geral:
        st.caption("Como a disponibilidade de dados variou entre o período anterior e o atual, hora a hora.")
        left, right = st.columns(2)

        with left:
            overall_sorted = overall_hourly.sort_values(["cenario", "hora"])
            overall_fig = go.Figure()
            for cenario_name, color in [("Antes", "#5470c6"), ("Depois", "#ee6666")]:
                cd = overall_sorted.loc[overall_sorted["cenario"] == cenario_name]
                if cd.empty:
                    continue
                overall_fig.add_trace(go.Scatter(
                    x=cd["hora"],
                    y=cd["pct_sem_dado"],
                    mode="lines+markers",
                    name=cenario_name,
                    line=dict(color=color),
                    marker=dict(size=6),
                    customdata=list(zip(
                        cd["minutos_sem_dado"].apply(fmt_duration),
                        cd["minutos_esperados"].apply(fmt_duration),
                        cd["dias_amostrados"].astype(int),
                        cd["min_sem_dado_por_dia"].apply(fmt_duration),
                    )),
                    hovertemplate=(
                        "<b>%{fullData.name}</b><br>"
                        "Hora: %{x}:00<br>"
                        "Sem dado: %{y:.1f}%<br>"
                        "Tempo sem dado: %{customdata[0]} de %{customdata[1]}<br>"
                        "Média por dia: %{customdata[3]}<br>"
                        "Dias amostrados: %{customdata[2]}"
                        "<extra></extra>"
                    ),
                ))
            update_hour_axis(overall_fig)
            overall_fig.update_layout(
                title="Percentual sem dado por hora do dia",
                xaxis_title="Hora do dia",
                yaxis_title="% sem dado",
                legend_title="",
                template=PLOTLY_TEMPLATE,
                hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
            )
            st.plotly_chart(overall_fig, use_container_width=True)

        with right:
            faixa_chart = px.bar(
                band_summary,
                x="faixa",
                y="pct_sem_dado",
                color="cenario",
                barmode="group",
                title="% sem dado: faixa crítica vs fora",
                template=PLOTLY_TEMPLATE,
            )
            faixa_chart.update_traces(
                hovertemplate="<b>%{fullData.name}</b><br>Faixa: %{x}<br>Sem dado: %{y:.2f}%<extra></extra>"
            )
            faixa_chart.update_layout(
                xaxis_title="Faixa horária",
                yaxis_title="% sem dado",
                legend_title="",
                hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
            )
            st.plotly_chart(faixa_chart, use_container_width=True)

        bottom_left, bottom_right = st.columns(2)

        with bottom_left:
            eventos_sensor_chart = px.bar(
                summary_long.sort_values(["sensor", "cenario"]),
                x="sensor",
                y="desconexoes_por_dia",
                color="cenario",
                barmode="group",
                title="Desconexões (>20 min) por dia — por sensor",
                template=PLOTLY_TEMPLATE,
            )
            eventos_sensor_chart.update_traces(
                hovertemplate="<b>%{fullData.name}</b><br>Sensor: %{x}<br>Desconexões/dia: %{y:.2f}<extra></extra>"
            )
            eventos_sensor_chart.update_layout(
                xaxis_title="Sensor",
                yaxis_title="Desconexões / dia",
                legend_title="",
                hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
            )
            st.plotly_chart(eventos_sensor_chart, use_container_width=True)

        with bottom_right:
            missing_sensor_chart = px.bar(
                summary_long.sort_values(["sensor", "cenario"]),
                x="sensor",
                y="min_sem_dado_por_dia",
                color="cenario",
                barmode="group",
                title="Minutos sem dado por dia — por sensor",
                template=PLOTLY_TEMPLATE,
            )
            missing_sensor_chart.update_traces(
                hovertemplate="<b>%{fullData.name}</b><br>Sensor: %{x}<br>Min. sem dado/dia: %{y:.2f}<extra></extra>"
            )
            missing_sensor_chart.update_layout(
                xaxis_title="Sensor",
                yaxis_title="Min. sem dado / dia",
                legend_title="",
                hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
            )
            st.plotly_chart(missing_sensor_chart, use_container_width=True)

        comparison_display = prepare_sensor_comparison_display(sensor_comparison)
        comparison_display = comparison_display.rename(columns={
            "sensor": "Sensor",
            "antes_pct_conectado": "Antes: % Conectado",
            "depois_pct_conectado": "Depois: % Conectado",
            "antes_min_sem_dado_dia": "Antes: Min s/ dado/dia",
            "depois_min_sem_dado_dia": "Depois: Min s/ dado/dia",
            "antes_desconexoes_dia": "Antes: Desconexões/dia",
            "depois_desconexoes_dia": "Depois: Desconexões/dia",
            "antes_tempo_desconectado_dia": "Antes: Tempo desc./dia",
            "depois_tempo_desconectado_dia": "Depois: Tempo desc./dia",
            "antes_maior_evento_min": "Antes: Maior evento (min)",
            "depois_maior_evento_min": "Depois: Maior evento (min)",
            "antes_pct_sem_dado_critico": "Antes: % s/ dado crítico",
            "depois_pct_sem_dado_critico": "Depois: % s/ dado crítico",
            "delta_pct_conectado_pp": "Δ % Conectado (p.p.)",
            "delta_min_sem_dado_dia": "Δ Min s/ dado/dia",
            "delta_desconexoes_dia": "Δ Desconexões/dia",
            "delta_pct_sem_dado_critico_pp": "Δ % s/ dado crítico (p.p.)",
            "reducao_min_sem_dado_dia": "Redução min s/ dado/dia",
        })
        st.dataframe(
            comparison_display,
            use_container_width=True,
            hide_index=True,
        )

    with tab_sensor:
        st.caption(
            "Análise detalhada por sensor: percentual sem dado hora a hora "
            "e leituras brutas mostrando lacunas de conexão."
        )

        # ── Gráficos individuais de % sem dado por sensor ──
        hourly_sensors = sorted(hourly_long["sensor"].unique().tolist()) if not hourly_long.empty else []

        for idx in range(0, len(hourly_sensors), 2):
            cols = st.columns(2)
            for col_idx, col in enumerate(cols):
                sensor_idx = idx + col_idx
                if sensor_idx >= len(hourly_sensors):
                    break
                s_name = hourly_sensors[sensor_idx]
                s_data = hourly_long.loc[hourly_long["sensor"] == s_name].sort_values(["cenario", "hora"])
                with col:
                    fig_s = go.Figure()
                    for cenario_name, color in [("Antes", "#5470c6"), ("Depois", "#ee6666")]:
                        cd = s_data.loc[s_data["cenario"] == cenario_name]
                        if cd.empty:
                            continue
                        fig_s.add_trace(go.Scatter(
                            x=cd["hora"],
                            y=cd["pct_sem_dado"],
                            mode="lines+markers",
                            name=cenario_name,
                            line=dict(color=color),
                            marker=dict(size=6),
                            customdata=list(zip(
                                cd["minutos_sem_dado"].apply(fmt_duration),
                                cd["minutos_esperados"].apply(fmt_duration),
                                cd["dias_amostrados"].astype(int),
                                cd["min_sem_dado_por_dia"].apply(fmt_duration),
                            )),
                            hovertemplate=(
                                "<b>%{fullData.name}</b><br>"
                                "Hora: %{x}:00<br>"
                                "Sem dado: %{y:.1f}%<br>"
                                "Tempo sem dado: %{customdata[0]} de %{customdata[1]}<br>"
                                "Média por dia: %{customdata[3]}<br>"
                                "Dias amostrados: %{customdata[2]}"
                                "<extra></extra>"
                            ),
                        ))
                    update_hour_axis(fig_s)
                    fig_s.update_layout(
                        title=s_name,
                        xaxis_title="Hora do dia",
                        yaxis_title="% sem dado",
                        legend_title="",
                        height=350,
                        template=PLOTLY_TEMPLATE,
                        hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
                    )
                    st.plotly_chart(fig_s, use_container_width=True)

        # ── Gráfico de leituras brutas (timeline) ──
        st.markdown("---")
        st.subheader("📡 Leituras brutas — visualização de lacunas")
        st.caption(
            "Cada ponto é uma leitura real do sensor. "
            "Onde a linha desaparece, o sensor ficou sem enviar dados (desconexão)."
        )

        before_raw = before_filtered["dados_brutos"].copy()
        after_raw = after_filtered["dados_brutos"].copy()

        if before_raw.empty and after_raw.empty:
            st.info("Não há dados brutos disponíveis para o filtro atual.")
        else:
            raw_sensors = sorted(
                set(
                    (before_raw["sensor"].unique().tolist() if not before_raw.empty else [])
                    + (after_raw["sensor"].unique().tolist() if not after_raw.empty else [])
                )
            )
            for raw_sensor in raw_sensors:
                st.markdown(f"#### {raw_sensor}")
                col_before, col_after = st.columns(2)

                with col_before:
                    sensor_before = before_raw.loc[before_raw["sensor"] == raw_sensor].copy() if not before_raw.empty else pd.DataFrame()
                    if sensor_before.empty:
                        st.info("Sem dados no período anterior.")
                    else:
                        sensor_before = sensor_before.sort_values("datahora")
                        fig_b = go.Figure()
                        fig_b.add_trace(go.Scatter(
                            x=sensor_before["datahora"],
                            y=sensor_before["valor"],
                            mode="lines",
                            line=dict(width=1, color="#5470c6"),
                            name="Antes",
                            connectgaps=False,
                            hovertemplate="<b>Antes</b><br>%{x|%d/%m %H:%M:%S}<br>Temperatura: %{y:.1f}<extra></extra>",
                        ))
                        fig_b.update_layout(
                            title=f"Antes — {before_filtered['period_label']}",
                            template=PLOTLY_TEMPLATE,
                            height=280,
                            margin=dict(l=50, r=10, t=40, b=35),
                            xaxis_title="",
                            yaxis_title="Leitura",
                            xaxis=dict(tickformat="%d/%m\n%H:%M"),
                            hoverlabel=dict(bgcolor="white", font_size=12, font_family="Arial"),
                        )
                        st.plotly_chart(fig_b, use_container_width=True)

                with col_after:
                    sensor_after = after_raw.loc[after_raw["sensor"] == raw_sensor].copy() if not after_raw.empty else pd.DataFrame()
                    if sensor_after.empty:
                        st.info("Sem dados no período atual.")
                    else:
                        sensor_after = sensor_after.sort_values("datahora")
                        fig_a = go.Figure()
                        fig_a.add_trace(go.Scatter(
                            x=sensor_after["datahora"],
                            y=sensor_after["valor"],
                            mode="lines",
                            line=dict(width=1, color="#ee6666"),
                            name="Depois",
                            connectgaps=False,
                            hovertemplate="<b>Depois</b><br>%{x|%d/%m %H:%M:%S}<br>Valor: %{y:.1f}<extra></extra>",
                        ))
                        fig_a.update_layout(
                            title=f"Depois — {after_filtered['period_label']}",
                            template=PLOTLY_TEMPLATE,
                            height=280,
                            margin=dict(l=50, r=10, t=40, b=35),
                            xaxis_title="",
                            yaxis_title="Leitura",
                            xaxis=dict(tickformat="%d/%m\n%H:%M"),
                            hoverlabel=dict(bgcolor="white", font_size=12, font_family="Arial"),
                        )
                        st.plotly_chart(fig_a, use_container_width=True)

    with tab_eventos:
        if events_long.empty:
            st.info("Não houve eventos de desconexão superiores a 20 minutos nos filtros atuais.")
        else:
            st.caption(
                "Cada evento é um período contínuo sem dados do sensor, com duração superior a 20 minutos."
            )

            before_events = events_long.loc[events_long["cenario"] == "Antes"]
            after_events = events_long.loc[events_long["cenario"] == "Depois"]

            ev_col1, ev_col2, ev_col3, ev_col4 = st.columns(4)
            with ev_col1:
                b_count = len(before_events)
                a_count = len(after_events)
                st.markdown(
                    f"""
                    <div style="background:#f8f9fa;border-radius:8px;padding:14px;text-align:center;">
                        <div style="font-size:0.8em;color:#666;">Total de eventos</div>
                        <div style="font-size:1.1em;margin-top:4px;">
                            <span style="color:#5470c6;font-weight:700;">Antes: {b_count}</span>
                            <span style="color:#ccc;"> | </span>
                            <span style="color:#ee6666;font-weight:700;">Depois: {a_count}</span>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with ev_col2:
                b_med = before_events["duracao_min"].median() if not before_events.empty else 0
                a_med = after_events["duracao_min"].median() if not after_events.empty else 0
                st.markdown(
                    f"""
                    <div style="background:#f8f9fa;border-radius:8px;padding:14px;text-align:center;">
                        <div style="font-size:0.8em;color:#666;">Duração mediana</div>
                        <div style="font-size:1.1em;margin-top:4px;">
                            <span style="color:#5470c6;font-weight:700;">Antes: {fmt_duration(b_med)}</span>
                            <span style="color:#ccc;"> | </span>
                            <span style="color:#ee6666;font-weight:700;">Depois: {fmt_duration(a_med)}</span>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with ev_col3:
                b_max = before_events["duracao_min"].max() if not before_events.empty else 0
                a_max = after_events["duracao_min"].max() if not after_events.empty else 0
                st.markdown(
                    f"""
                    <div style="background:#f8f9fa;border-radius:8px;padding:14px;text-align:center;">
                        <div style="font-size:0.8em;color:#666;">Maior evento</div>
                        <div style="font-size:1.1em;margin-top:4px;">
                            <span style="color:#5470c6;font-weight:700;">Antes: {fmt_duration(b_max)}</span>
                            <span style="color:#ccc;"> | </span>
                            <span style="color:#ee6666;font-weight:700;">Depois: {fmt_duration(a_max)}</span>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with ev_col4:
                b_crit = before_events["periodo_critico"].sum() if not before_events.empty and "periodo_critico" in before_events.columns else 0
                a_crit = after_events["periodo_critico"].sum() if not after_events.empty and "periodo_critico" in after_events.columns else 0
                st.markdown(
                    f"""
                    <div style="background:#f8f9fa;border-radius:8px;padding:14px;text-align:center;">
                        <div style="font-size:0.8em;color:#666;">Entre 20h e 8h</div>
                        <div style="font-size:1.1em;margin-top:4px;">
                            <span style="color:#5470c6;font-weight:700;">Antes: {int(b_crit)}</span>
                            <span style="color:#ccc;"> | </span>
                            <span style="color:#ee6666;font-weight:700;">Depois: {int(a_crit)}</span>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            st.markdown("")

            ev_left, ev_right = st.columns(2)

            with ev_left:
                sensor_counts = (
                    events_long.groupby(["cenario", "sensor"], as_index=False)
                    .size()
                    .rename(columns={"size": "eventos"})
                    .sort_values(["sensor", "cenario"])
                )
                fig_sensor_ev = px.bar(
                    sensor_counts,
                    x="sensor",
                    y="eventos",
                    color="cenario",
                    barmode="group",
                    title="Quantidade de eventos por sensor",
                    template=PLOTLY_TEMPLATE,
                    color_discrete_map={"Antes": "#5470c6", "Depois": "#ee6666"},
                )
                fig_sensor_ev.update_traces(
                    hovertemplate="<b>%{fullData.name}</b><br>Sensor: %{x}<br>Eventos: %{y}<extra></extra>"
                )
                fig_sensor_ev.update_layout(
                    xaxis_title="Sensor",
                    yaxis_title="Eventos",
                    legend_title="",
                    hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
                )
                st.plotly_chart(fig_sensor_ev, use_container_width=True)

            with ev_right:
                sensor_dur = (
                    events_long.groupby(["cenario", "sensor"], as_index=False)
                    .agg(tempo_total=("duracao_min", "sum"))
                    .sort_values(["sensor", "cenario"])
                )
                fig_sensor_dur = px.bar(
                    sensor_dur,
                    x="sensor",
                    y="tempo_total",
                    color="cenario",
                    barmode="group",
                    title="Tempo total offline por sensor (min)",
                    template=PLOTLY_TEMPLATE,
                    color_discrete_map={"Antes": "#5470c6", "Depois": "#ee6666"},
                )
                fig_sensor_dur.update_traces(
                    hovertemplate="<b>%{fullData.name}</b><br>Sensor: %{x}<br>Tempo offline: %{y:.0f} min<extra></extra>"
                )
                fig_sensor_dur.update_layout(
                    xaxis_title="Sensor",
                    yaxis_title="Tempo total (min)",
                    legend_title="",
                    hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
                )
                st.plotly_chart(fig_sensor_dur, use_container_width=True)

            st.markdown("---")
            st.subheader("Linha do tempo dos eventos")
            st.caption(
                "Cada barra é uma desconexão. O comprimento indica a duração. "
                "Passe o mouse para ver detalhes."
            )

            timeline_data = events_long.copy()
            if {"inicio_desconexao", "fim_desconexao"}.issubset(timeline_data.columns):
                timeline_data = timeline_data.dropna(subset=["inicio_desconexao", "fim_desconexao"])
                for cenario_name, cenario_color, cenario_label in [
                    ("Antes", "#5470c6", f"Antes — {before_filtered['period_label']}"),
                    ("Depois", "#ee6666", f"Depois — {after_filtered['period_label']}"),
                ]:
                    cenario_ev = timeline_data.loc[timeline_data["cenario"] == cenario_name].copy()
                    if cenario_ev.empty:
                        st.caption(f"{cenario_label}: nenhum evento.")
                        continue

                    cenario_ev["duracao_fmt"] = cenario_ev["duracao_min"].apply(fmt_duration)
                    cenario_ev["inicio_fmt"] = cenario_ev["inicio_desconexao"].dt.strftime("%d/%m %H:%M")
                    cenario_ev["fim_fmt"] = cenario_ev["fim_desconexao"].dt.strftime("%d/%m %H:%M")
                    cenario_ev["critico_label"] = cenario_ev["periodo_critico"].map(
                        {True: "⚠️ Horário crítico", False: ""}
                    ).fillna("")

                    fig_tl = px.timeline(
                        cenario_ev,
                        x_start="inicio_desconexao",
                        x_end="fim_desconexao",
                        y="sensor",
                        title=cenario_label,
                        template=PLOTLY_TEMPLATE,
                        custom_data=["inicio_fmt", "fim_fmt", "duracao_fmt", "critico_label"],
                    )
                    fig_tl.update_traces(
                        marker_color=cenario_color,
                        opacity=0.85,
                        hovertemplate=(
                            "<b>%{y}</b><br>"
                            "Início: %{customdata[0]}<br>"
                            "Fim: %{customdata[1]}<br>"
                            "Duração: %{customdata[2]}<br>"
                            "%{customdata[3]}"
                            "<extra></extra>"
                        ),
                    )
                    unique_sensors = cenario_ev["sensor"].nunique()
                    fig_tl.update_layout(
                        height=max(200, 70 * unique_sensors + 80),
                        margin=dict(l=10, r=10, t=40, b=30),
                        xaxis_title="",
                        yaxis_title="",
                        yaxis=dict(categoryorder="category ascending"),
                        hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
                    )
                    st.plotly_chart(fig_tl, use_container_width=True)

            st.markdown("---")
            st.subheader("Detalhamento dos eventos")
            events_view = events_long[
                [
                    "cenario",
                    "sensor",
                    "inicio_desconexao",
                    "fim_desconexao",
                    "duracao_min",
                    "periodo_critico",
                ]
            ].copy()
            events_view["duracao_fmt"] = events_view["duracao_min"].apply(fmt_duration)
            events_view["inicio_desconexao"] = events_view["inicio_desconexao"].dt.strftime("%d/%m/%Y %H:%M")
            events_view["fim_desconexao"] = events_view["fim_desconexao"].dt.strftime("%d/%m/%Y %H:%M")
            events_view["periodo_critico"] = events_view["periodo_critico"].map({True: "⚠️ Sim", False: "Não"})
            events_view = events_view.sort_values(["cenario", "duracao_min"], ascending=[True, False])
            events_view = events_view.drop(columns=["duracao_min"])
            events_view = events_view.rename(columns={
                "cenario": "Cenário",
                "sensor": "Sensor",
                "inicio_desconexao": "Início",
                "fim_desconexao": "Fim",
                "duracao_fmt": "Duração",
                "periodo_critico": "Horário Crítico?",
            })
            st.dataframe(events_view, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    if st.runtime.exists():
        render_dashboard()
    else:
        print("Este arquivo e um app Streamlit.")
        print("Execute assim:")
        print("  streamlit run .\\dashboard_ubidots_comparativo.py")
