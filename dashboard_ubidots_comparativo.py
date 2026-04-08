from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import re

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import math


SCRIPT_DIR = Path(__file__).resolve().parent
LOCAL_SAGIL_DIR = Path(r"C:\Users\sagil\Downloads\sagil_")
LOCAL_AFTER_DIR = LOCAL_SAGIL_DIR / "depois" / "2026-03-26_a_2026-04-05"
LOCAL_BEFORE_ROOT = LOCAL_SAGIL_DIR / "antes"

# Caminhos para dados do concorrente (Excel)
LOCAL_COMPETITOR_DIR = Path(r"C:\Users\sagil\Downloads\syos")
COMPETITOR_BEFORE_FILE = "ANTES_SyOS-20-08-25_até_31-08-25-.xlsx"
COMPETITOR_AFTER_FILE = "DEPOIS_SyOS-26-03-26_até_05-04-26-.xlsx"

# Caminhos para dados NOSSO (Excel - exportados da Ubidots)
REPO_AFTER_DIR = SCRIPT_DIR / "saida_ubidots_analise"
REPO_BEFORE_ROOT = SCRIPT_DIR / "antes"
REPO_COMPETITOR_DIR = SCRIPT_DIR / "syos"
#NOSSO_BEFORE_FILE = "ANTES.xlsx"
#NOSSO_AFTER_FILE = "DEPOIS.xlsx"


def first_existing_path(*candidates: Path) -> Path:
    for path in candidates:
        if path.exists():
            return path
    return candidates[0]


AFTER_DIR = first_existing_path(LOCAL_AFTER_DIR, REPO_AFTER_DIR)
BEFORE_ROOT = first_existing_path(LOCAL_BEFORE_ROOT, REPO_BEFORE_ROOT)
COMPETITOR_DIR = first_existing_path(LOCAL_COMPETITOR_DIR, REPO_COMPETITOR_DIR)
NOSSO_DIR = first_existing_path(LOCAL_SAGIL_DIR, SCRIPT_DIR)

PLOTLY_TEMPLATE = "plotly_white"
CRITICAL_HOURS = {20, 21, 22, 23, 0, 1, 2, 3, 4, 5, 6, 7}
MAIN_GRID_FREQ = "1min"
BENCHMARK_GRID_FREQ = "5min"
DISCONNECT_THRESHOLD_MINUTES = 20

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

# =========================================================
# CONFIGURAÇÕES
# =========================================================

TIMEZONE = "America/Sao_Paulo"


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
        return f"{hours}h e {mins}min"
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


# =========================================================
# FUNÇÕES AUXILIARES CSV
# =========================================================


def read_csv_flex(path: Path) -> pd.DataFrame:
    """Lê CSV com suporte a múltiplos encodings."""
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

    if "timestamp" in df.columns:
        exact_dt = (
            pd.to_datetime(df["timestamp"], unit="ms", utc=True, errors="coerce")
            .dt.tz_convert(TIMEZONE)
            .dt.tz_localize(None)
        )
        if "datahora" in df.columns:
            df["datahora"] = exact_dt.fillna(df["datahora"])
        else:
            df["datahora"] = exact_dt

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


def event_touches_critical_hours(start: pd.Timestamp, end: pd.Timestamp) -> bool:
    if pd.isna(start) or pd.isna(end) or start > end:
        return False
    return any(
        timestamp.hour in CRITICAL_HOURS
        for timestamp in pd.date_range(start=start, end=end, freq="1min")
    )


def resolve_period_bounds(
    base_dir: Path,
    metricas_periodo: pd.DataFrame,
    dados_brutos: pd.DataFrame,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    match = re.search(r"(\d{4}-\d{2}-\d{2})_a_(\d{4}-\d{2}-\d{2})", base_dir.name)
    if match:
        start = pd.Timestamp(match.group(1))
        end = pd.Timestamp(match.group(2)) + pd.Timedelta(hours=23, minutes=59)
        return start, end

    if not metricas_periodo.empty and {"inicio_periodo", "fim_periodo"}.issubset(metricas_periodo.columns):
        start = metricas_periodo["inicio_periodo"].dropna().min()
        end = metricas_periodo["fim_periodo"].dropna().max()
        if pd.notna(start) and pd.notna(end):
            return pd.Timestamp(start), pd.Timestamp(end)

    if not dados_brutos.empty and "datahora" in dados_brutos.columns:
        start = dados_brutos["datahora"].dropna().min()
        end = dados_brutos["datahora"].dropna().max()
        if pd.notna(start) and pd.notna(end):
            return pd.Timestamp(start).normalize(), pd.Timestamp(end).normalize() + pd.Timedelta(hours=23, minutes=59)

    return None, None


def build_available_days(period_start: pd.Timestamp | None, period_end: pd.Timestamp | None) -> list[pd.Timestamp]:
    if period_start is None or period_end is None:
        return []
    return list(pd.date_range(start=period_start.normalize(), end=period_end.normalize(), freq="D"))


def build_time_grid_from_raw(
    raw_sensor: pd.DataFrame,
    period_start: pd.Timestamp,
    period_end: pd.Timestamp,
    freq: str = MAIN_GRID_FREQ,
) -> pd.DataFrame:
    grid = pd.date_range(start=period_start, end=period_end, freq=freq)
    grid_df = pd.DataFrame({"datahora": grid})

    if raw_sensor.empty:
        grid_df["valor"] = math.nan
        return grid_df

    series = raw_sensor.copy()
    if "valor" not in series.columns or "datahora" not in series.columns:
        grid_df["valor"] = math.nan
        return grid_df

    series = series.dropna(subset=["datahora"]).sort_values("datahora")
    if series.empty:
        grid_df["valor"] = math.nan
        return grid_df

    resampled = (
        series.set_index("datahora")[["valor"]]
        .resample(freq)
        .mean()
        .reindex(grid)
        .rename_axis("datahora")
        .reset_index()
    )
    return resampled


def extract_disconnect_events(
    grid_df: pd.DataFrame,
    min_gap_minutes: int = DISCONNECT_THRESHOLD_MINUTES,
    slot_minutes: int = 1,
) -> pd.DataFrame:
    cols = ["inicio_desconexao", "fim_desconexao", "duracao_min", "periodo_critico"]
    if grid_df.empty or "datahora" not in grid_df.columns:
        return pd.DataFrame(columns=cols)

    df = grid_df.dropna(subset=["datahora"]).sort_values("datahora").reset_index(drop=True)
    if df.empty:
        return pd.DataFrame(columns=cols)

    sem_dado = df["valor"].isna()
    events: list[dict[str, object]] = []
    in_gap = False
    gap_start = None
    gap_end = None
    gap_slots = 0

    for i, row in df.iterrows():
        current_ts = row["datahora"]
        if sem_dado.iloc[i]:
            if not in_gap:
                in_gap = True
                gap_start = current_ts
                gap_slots = 1
            else:
                gap_slots += 1
            gap_end = current_ts
            continue

        if in_gap and gap_start is not None and gap_end is not None:
            duration_minutes = gap_slots * slot_minutes
            if duration_minutes > min_gap_minutes:
                events.append(
                    {
                        "inicio_desconexao": gap_start,
                        "fim_desconexao": gap_end,
                        "duracao_min": duration_minutes,
                        "periodo_critico": event_touches_critical_hours(gap_start, gap_end),
                    }
                )
            in_gap = False
            gap_start = None
            gap_end = None
            gap_slots = 0

    if in_gap and gap_start is not None and gap_end is not None:
        duration_minutes = gap_slots * slot_minutes
        if duration_minutes > min_gap_minutes:
            events.append(
                {
                    "inicio_desconexao": gap_start,
                    "fim_desconexao": gap_end,
                    "duracao_min": duration_minutes,
                    "periodo_critico": event_touches_critical_hours(gap_start, gap_end),
                }
            )

    return pd.DataFrame(events, columns=cols) if events else pd.DataFrame(columns=cols)


def summarize_events_in_interval(
    events_df: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> dict[str, float]:
    if events_df.empty:
        return {
            "quantidade": 0,
            "menor_min": 0.0,
            "maior_min": 0.0,
            "media_min": 0.0,
            "tempo_total_min": 0.0,
            "criticas": 0,
        }

    overlap_durations: list[int] = []
    critical_count = 0

    for _, event in events_df.iterrows():
        overlap_start = max(event["inicio_desconexao"], start)
        overlap_end = min(event["fim_desconexao"], end)
        if overlap_start > overlap_end:
            continue

        duration = int((overlap_end - overlap_start).total_seconds() / 60) + 1
        overlap_durations.append(duration)
        if event_touches_critical_hours(overlap_start, overlap_end):
            critical_count += 1

    if not overlap_durations:
        return {
            "quantidade": 0,
            "menor_min": 0.0,
            "maior_min": 0.0,
            "media_min": 0.0,
            "tempo_total_min": 0.0,
            "criticas": 0,
        }

    return {
        "quantidade": len(overlap_durations),
        "menor_min": round(float(min(overlap_durations)), 2),
        "maior_min": round(float(max(overlap_durations)), 2),
        "media_min": round(float(sum(overlap_durations) / len(overlap_durations)), 2),
        "tempo_total_min": round(float(sum(overlap_durations)), 2),
        "criticas": critical_count,
    }


def compute_period_metrics_from_grid(
    sensor: str,
    grid_df: pd.DataFrame,
    events_df: pd.DataFrame,
) -> pd.DataFrame:
    total_minutos = len(grid_df)
    minutos_com_dado = int(grid_df["valor"].notna().sum()) if "valor" in grid_df.columns else 0
    minutos_sem_dado = int(grid_df["valor"].isna().sum()) if "valor" in grid_df.columns else 0
    pct_conectado = round((minutos_com_dado / total_minutos) * 100, 2) if total_minutos else 0
    pct_desconectado = round((minutos_sem_dado / total_minutos) * 100, 2) if total_minutos else 0
    inicio_periodo = grid_df["datahora"].min() if not grid_df.empty else pd.NaT
    fim_periodo = grid_df["datahora"].max() if not grid_df.empty else pd.NaT
    event_summary = summarize_events_in_interval(events_df, inicio_periodo, fim_periodo)

    return pd.DataFrame(
        [
            {
                "variavel": sensor,
                "sensor": sensor,
                "inicio_periodo": inicio_periodo,
                "fim_periodo": fim_periodo,
                "total_minutos_esperados": total_minutos,
                "minutos_com_dado": minutos_com_dado,
                "minutos_sem_dado": minutos_sem_dado,
                "percentual_conectado": pct_conectado,
                "percentual_desconectado": pct_desconectado,
                "quantidade_desconexoes_gt_20min": event_summary["quantidade"],
                "menor_desconexao_min": event_summary["menor_min"],
                "maior_desconexao_min": event_summary["maior_min"],
                "media_desconexao_min": event_summary["media_min"],
                "tempo_total_desconectado_min": event_summary["tempo_total_min"],
                "desconexoes_no_periodo_critico_20h_08h": event_summary["criticas"],
            }
        ]
    )


def compute_daily_metrics_from_grid(
    sensor: str,
    grid_df: pd.DataFrame,
    events_df: pd.DataFrame,
) -> pd.DataFrame:
    if grid_df.empty:
        return pd.DataFrame()

    frame = grid_df.copy()
    frame["dia"] = frame["datahora"].dt.normalize()
    rows = []

    for day, group in frame.groupby("dia", sort=True):
        start = group["datahora"].min()
        end = group["datahora"].max()
        event_summary = summarize_events_in_interval(events_df, start, end)
        total_minutos = len(group)
        minutos_com_dado = int(group["valor"].notna().sum())
        minutos_sem_dado = int(group["valor"].isna().sum())
        pct_conectado = round((minutos_com_dado / total_minutos) * 100, 2) if total_minutos else 0
        pct_desconectado = round((minutos_sem_dado / total_minutos) * 100, 2) if total_minutos else 0

        rows.append(
            {
                "variavel": sensor,
                "sensor": sensor,
                "dia": day,
                "total_minutos_esperados": total_minutos,
                "minutos_com_dado": minutos_com_dado,
                "minutos_sem_dado": minutos_sem_dado,
                "percentual_conectado": pct_conectado,
                "percentual_desconectado": pct_desconectado,
                "quantidade_desconexoes_gt_20min": event_summary["quantidade"],
                "menor_desconexao_min": event_summary["menor_min"],
                "maior_desconexao_min": event_summary["maior_min"],
                "media_desconexao_min": event_summary["media_min"],
                "tempo_total_desconectado_min": event_summary["tempo_total_min"],
                "desconexoes_no_periodo_critico_20h_08h": event_summary["criticas"],
            }
        )

    return pd.DataFrame(rows)


def rebuild_dataset_from_raw(
    combined: dict[str, pd.DataFrame],
    sensor_names: list[str],
    period_start: pd.Timestamp | None,
    period_end: pd.Timestamp | None,
    scenario: str,
) -> dict[str, pd.DataFrame]:
    if period_start is None or period_end is None:
        return combined

    raw_all = combined.get("dados_brutos", pd.DataFrame()).copy()
    if not raw_all.empty and "datahora" in raw_all.columns:
        raw_all = raw_all.dropna(subset=["datahora"]).sort_values(["sensor", "datahora"]).reset_index(drop=True)

    all_normalized = []
    all_events = []
    all_period_metrics = []
    all_daily_metrics = []

    for sensor in sensor_names:
        if not raw_all.empty and "sensor" in raw_all.columns:
            raw_sensor = raw_all.loc[raw_all["sensor"] == sensor].copy()
        else:
            raw_sensor = pd.DataFrame(columns=["datahora", "valor", "sensor"])

        normalized_sensor = build_time_grid_from_raw(raw_sensor, period_start, period_end, freq=MAIN_GRID_FREQ)
        normalized_sensor["sensor"] = sensor
        normalized_sensor["cenario"] = scenario
        all_normalized.append(normalized_sensor)

        sensor_events = extract_disconnect_events(
            normalized_sensor[["datahora", "valor"]],
            min_gap_minutes=DISCONNECT_THRESHOLD_MINUTES,
            slot_minutes=1,
        )
        if not sensor_events.empty:
            sensor_events["sensor"] = sensor
            sensor_events["cenario"] = scenario
            all_events.append(sensor_events)

        period_metrics = compute_period_metrics_from_grid(sensor, normalized_sensor, sensor_events)
        period_metrics["cenario"] = scenario
        all_period_metrics.append(period_metrics)

        daily_metrics = compute_daily_metrics_from_grid(sensor, normalized_sensor, sensor_events)
        if not daily_metrics.empty:
            daily_metrics["cenario"] = scenario
            all_daily_metrics.append(daily_metrics)

    combined["dados_normalizados"] = (
        pd.concat(all_normalized, ignore_index=True) if all_normalized else pd.DataFrame()
    )
    combined["eventos"] = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()
    combined["metricas_periodo"] = (
        pd.concat(all_period_metrics, ignore_index=True) if all_period_metrics else pd.DataFrame()
    )
    combined["metricas_por_dia"] = (
        pd.concat(all_daily_metrics, ignore_index=True) if all_daily_metrics else pd.DataFrame()
    )
    combined["resumo_geral"] = combined["metricas_periodo"].copy()
    return combined


def infer_slot_minutes(dados_normalizados: pd.DataFrame) -> int:
    if dados_normalizados.empty or "datahora" not in dados_normalizados.columns:
        return 1

    frame = dados_normalizados.dropna(subset=["datahora"]).sort_values(["sensor", "datahora"]).copy()
    if frame.empty:
        return 1

    diffs = frame.groupby("sensor")["datahora"].diff().dropna().dt.total_seconds() / 60
    diffs = diffs[diffs > 0]
    if diffs.empty:
        return 1

    return max(1, int(round(float(diffs.median()))))


def load_competitor_data(excel_file: Path, period: str) -> pd.DataFrame:
    """
    Carrega dados do concorrente a partir de arquivo Excel.

    Colunas esperadas: Data, Apelido do balcão, Temperatura (entre outras colunas)
    Filtra apenas para linhas onde "Apelido do balcão" contém "Balcão" (não Câmara)

    Args:
        excel_file: Caminho do arquivo Excel (ANTES_SyOS-*.xlsx ou DEPOIS_SyOS-*.xlsx)
        period: "Concorrente - Antes" ou "Concorrente - Depois"

    Returns:
        DataFrame normalizado com colunas: datahora, sensor, valor, periodo
    """
    if not excel_file.exists():
        st.warning(f"❌ Arquivo não encontrado: {excel_file}")
        return pd.DataFrame()

    try:
        import re

        # Lê o Excel
        df = pd.read_excel(excel_file)

        # Verifica colunas necessárias
        required_cols = ["Data", "Apelido do balcão", "Temperatura"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            st.error(f"❌ Colunas não encontradas no Excel: {missing}")
            st.warning(f"Colunas disponíveis: {list(df.columns)}")
            return pd.DataFrame()

        # Filtra apenas linhas que contêm "Balcão" e NÃO contêm "Câmara"
        mask = (
            df["Apelido do balcão"]
            .astype(str)
            .str.contains("Balcão", case=False, na=False)
        ) & (
            ~df["Apelido do balcão"]
            .astype(str)
            .str.contains("Câmara", case=False, na=False)
        )
        df = df[mask].copy()

        if df.empty:
            st.warning(
                f"⚠️ Nenhuma linha encontrada com critérios de filtro (contém 'Balcão', não contém 'Câmara')"
            )
            return pd.DataFrame()

        # Renomeia e seleciona apenas as colunas necessárias
        df = df[["Data", "Apelido do balcão", "Temperatura"]].copy()
        df.columns = ["datahora", "sensor", "valor"]

        # Converte data para datetime
        df["datahora"] = pd.to_datetime(df["datahora"], dayfirst=True, errors="coerce")

        # Converte temperatura para numérica
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

        # Remove linhas com datas ou valores inválidos
        df = df.dropna(subset=["datahora", "valor"])

        # Limpa o nome do sensor: extrai a parte "Balcão xxx"
        # Exemplo: "00-02 31-84-13 - Balcão Vácuo Resfriados -B0FDE" -> "Balcão Vácuo Resfriados"
        df["sensor"] = df["sensor"].apply(
            lambda x: (
                re.search(r"-\s*(Balcão[^-]+)\s*-", x).group(1).strip()
                if re.search(r"-\s*(Balcão[^-]+)\s*-", x)
                else x
            )
        )

        # Adiciona período
        df["periodo"] = period

        # Ordena por sensor e data
        df = df.sort_values(["sensor", "datahora"]).reset_index(drop=True)

        return df

    except Exception as e:
        st.error(f"❌ Erro ao ler arquivo Excel: {e}")
        import traceback

        st.error(f"Detalhes: {traceback.format_exc()}")
        return pd.DataFrame()


def load_competitor_before() -> pd.DataFrame:
    """Carrega dados do concorrente período ANTES."""
    excel_file = COMPETITOR_DIR / COMPETITOR_BEFORE_FILE
    return load_competitor_data(excel_file, "Concorrente - Antes")


def load_competitor_after() -> pd.DataFrame:
    """Carrega dados do concorrente período DEPOIS."""
    excel_file = COMPETITOR_DIR / COMPETITOR_AFTER_FILE
    return load_competitor_data(excel_file, "Concorrente - Depois")


def load_nosso_by_period(period_type: str) -> pd.DataFrame:
    """
    Carrega dados NOSSO a partir de CSVs organizados em pastas por sensor.

    Estrutura esperada:
    NOSSO_DIR/
    ├── antes/
    │   └── 2025-08-20_a_2025-08-31/
    │       ├── resumo_geral.csv
    │       ├── sensor1/
    │       │   ├── dados_normalizados_1min.csv
    │       │   ├── dados_brutos.csv
    │       │   ├── metricas_periodo.csv
    │       │   └── metricas_por_dia.csv
    │       └── sensor2/...
    └── depois/
        └── 2026-03-26_a_2026-04-05/...

    Args:
        period_type: "antes" ou "depois"

    Returns:
        DataFrame com colunas: datahora, sensor, valor, periodo
    """
    date_folder = resolve_sagil_period_dir(period_type)
    if date_folder is None or not date_folder.exists():
        return pd.DataFrame()

    # Define o rótulo do período
    periodo_label = "NOSSO - Antes" if period_type == "antes" else "NOSSO - Depois"

    # Coleta dados de todos os sensores
    all_data = []

    for sensor_folder in date_folder.iterdir():
        if not sensor_folder.is_dir() or sensor_folder.name == "resumo_geral.csv":
            continue

        sensor_name = sensor_folder.name
        csv_file = sensor_folder / "dados_normalizados_1min.csv"

        if csv_file.exists():
            try:
                df = read_csv_flex(csv_file)
                df = df.copy()

                # Normaliza nomes de colunas
                df.columns = [col.strip().lower() for col in df.columns]

                # Se a coluna é "datahora", renomeia para formato consistente
                if "datahora" in df.columns:
                    df["datahora"] = pd.to_datetime(
                        df["datahora"], dayfirst=True, errors="coerce"
                    )

                # Garante que tem coluna "valor"
                if "valor" in df.columns:
                    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
                    df["sensor"] = sensor_name
                    df["periodo"] = periodo_label  # ← Adiciona período
                    all_data.append(df[["datahora", "sensor", "valor", "periodo"]])
            except Exception:
                pass

    if not all_data:
        return pd.DataFrame()

    result = pd.concat(all_data, ignore_index=True)
    result = result.dropna(subset=["datahora", "valor"])
    result = result.sort_values(["sensor", "datahora"]).reset_index(drop=True)

    return result


# =========================================================
# PROCESSAMENTO DE DADOS
# =========================================================
# PROCESSAMENTO DE DADOS
# =========================================================


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


def resolve_sagil_period_dir(period_type: str) -> Path | None:
    standard_root = NOSSO_DIR / period_type
    if standard_root.exists():
        date_folders = sorted(path for path in standard_root.iterdir() if path.is_dir())
        if date_folders:
            return date_folders[-1]
        if (standard_root / "resumo_geral.csv").exists():
            return standard_root

    if period_type == "antes":
        before_periods = list_before_periods(REPO_BEFORE_ROOT)
        if before_periods:
            return before_periods[-1]

    if period_type == "depois" and REPO_AFTER_DIR.exists():
        return REPO_AFTER_DIR

    return None


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

    sensor_names = sorted(
        {
            row["sensor"]
            for row in inventory_rows
            if row["sensor"] != "geral"
        }
    )
    period_start, period_end = resolve_period_bounds(
        base_dir,
        combined["metricas_periodo"],
        combined["dados_brutos"],
    )
    combined = rebuild_dataset_from_raw(
        combined,
        sensor_names=sensor_names,
        period_start=period_start,
        period_end=period_end,
        scenario=scenario,
    )
    available_days = build_available_days(period_start, period_end)

    return {
        "cenario": scenario,
        "base_dir": str(base_dir.resolve()),
        "period_label": build_period_label_from_days(available_days),
        "available_days": available_days,
        "inventory": pd.DataFrame(inventory_rows),
        "resumo_geral": resumo_geral,
        **combined,
    }


def build_dataset_cache_token(base_dir_str: str) -> tuple[tuple[str, int, int], ...]:
    """Assinatura do diretório para invalidar cache quando os arquivos mudarem."""
    base_dir = Path(base_dir_str)
    if not base_dir.exists():
        return tuple()

    file_tokens: list[tuple[str, int, int]] = []
    for file_path in sorted(path for path in base_dir.rglob("*") if path.is_file()):
        stat = file_path.stat()
        file_tokens.append(
            (
                str(file_path.relative_to(base_dir)).replace("\\", "/"),
                int(stat.st_mtime_ns),
                int(stat.st_size),
            )
        )
    return tuple(file_tokens)


def _load_dataset_cached(
    base_dir_str: str,
    scenario: str,
    cache_token: tuple[tuple[str, int, int], ...],
) -> dict[str, object]:
    return _load_dataset(base_dir_str, scenario)


if st.runtime.exists():
    _load_dataset_cached = st.cache_data(show_spinner=False)(_load_dataset_cached)

    def load_dataset(base_dir_str: str, scenario: str) -> dict[str, object]:
        cache_token = build_dataset_cache_token(base_dir_str)
        return _load_dataset_cached(base_dir_str, scenario, cache_token)

else:

    def load_dataset(base_dir_str: str, scenario: str) -> dict[str, object]:
        return _load_dataset(base_dir_str, scenario)


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


def count_sample_days(
    metricas_por_dia: pd.DataFrame,
    start_date: pd.Timestamp = None,
    end_date: pd.Timestamp = None,
) -> int:
    """Conta dias no intervalo [start_date, end_date]. Se datas não fornecidas, conta dias com dados."""
    if start_date is not None and end_date is not None:
        return (end_date - start_date).days + 1

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

    filtered["sample_days"] = count_sample_days(
        filtered["metricas_por_dia"], start_date, end_date
    )
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
            maior_evento_min=("maior_desconexao_min", "max"),
            media_evento_min=("media_desconexao_min", "mean"),
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
    slot_minutes = infer_slot_minutes(frame)

    frame["dia_ref"] = frame["datahora"].dt.normalize()
    frame["hora"] = frame["datahora"].dt.hour
    frame["com_dado"] = frame["valor"].notna().astype(int)

    profile = frame.groupby(["sensor", "hora"], as_index=False).agg(
        dias_amostrados=("dia_ref", "nunique"),
        total_slots=("com_dado", "size"),
        slots_com_dado=("com_dado", "sum"),
    )

    profile["minutos_esperados"] = profile["total_slots"] * slot_minutes
    profile["minutos_com_dado"] = profile["slots_com_dado"] * slot_minutes
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
    slot_minutes = infer_slot_minutes(frame)

    frame["dia_ref"] = frame["datahora"].dt.normalize()
    frame["hora"] = frame["datahora"].dt.hour
    frame["com_dado"] = frame["valor"].notna().astype(int)

    profile = frame.groupby("hora", as_index=False).agg(
        dias_amostrados=("dia_ref", "nunique"),
        total_slots=("com_dado", "size"),
        slots_com_dado=("com_dado", "sum"),
    )

    profile["minutos_esperados"] = profile["total_slots"] * slot_minutes
    profile["minutos_com_dado"] = profile["slots_com_dado"] * slot_minutes
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

    # Verifica se as colunas necessárias existem
    required_cols = [
        "total_minutos_esperados",
        "minutos_com_dado",
        "minutos_sem_dado",
        "desconexoes_gt_20min",
        "tempo_total_desconectado_min",
    ]
    for col in required_cols:
        if col not in sensor_summary.columns:
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


# =========================================================
# BENCHMARK: Processamento para comparação Sagil vs SyOS
# =========================================================

BENCHMARK_SENSOR_PAIRS = [
    {
        "sagil": "balcao_congelado_calibrado",
        "syos": "00-02 31-84-15 - Balcão Vácuo Congelados - B2B5A",
        "titulo": "Balcão Congelado",
    },
    {
        "sagil": "balcao_resfriado_1_calibrado",
        "syos": "00-02 31-84-13 - Balcão Vácuo Resfriados -B0FDE",
        "titulo": "Balcão Resfriado 1",
    },
    {
        "sagil": "balcao_resfriado_2",
        "syos": "00-02 31-84-14 - Balcão Vácuo Resfriados - B67F1",
        "titulo": "Balcão Resfriado 2",
    },
]

BENCHMARK_PERIODS = {
    "antes": {
        "start": pd.Timestamp("2025-08-20"),
        "end": pd.Timestamp("2025-08-31 23:55:00"),
        "label": "20/08/2025 a 31/08/2025",
    },
    "depois": {
        "start": pd.Timestamp("2026-03-26"),
        "end": pd.Timestamp("2026-04-05 23:55:00"),
        "label": "26/03/2026 a 05/04/2026",
    },
}


def _load_sagil_benchmark(period_type: str) -> pd.DataFrame:
    """Carrega dados brutos da Sagil para benchmark."""
    date_folder = resolve_sagil_period_dir(period_type)
    if date_folder is None or not date_folder.exists():
        return pd.DataFrame()
    all_data = []
    for sensor_folder in date_folder.iterdir():
        if not sensor_folder.is_dir():
            continue
        sensor_name = sensor_folder.name
        csv_file = sensor_folder / "dados_brutos.csv"
        if csv_file.exists():
            try:
                df = maybe_read_csv(csv_file, sensor=sensor_name)
                if {"datahora", "valor"}.issubset(df.columns):
                    all_data.append(df[["datahora", "sensor", "valor"]])
            except Exception:
                pass
    if not all_data:
        return pd.DataFrame()
    result = pd.concat(all_data, ignore_index=True)
    result = result.dropna(subset=["datahora", "valor"])
    return result.sort_values(["sensor", "datahora"]).reset_index(drop=True)


def _load_syos_benchmark(excel_file: Path) -> pd.DataFrame:
    """Carrega dados SyOS mantendo nomes originais dos sensores."""
    if not excel_file.exists():
        return pd.DataFrame()
    try:
        df = pd.read_excel(excel_file)
        required = ["Data", "Apelido do balcão", "Temperatura"]
        if not all(c in df.columns for c in required):
            return pd.DataFrame()
        mask = (
            df["Apelido do balcão"].str.contains("Balcão", case=False, na=False)
            & ~df["Apelido do balcão"].str.contains("Câmara", case=False, na=False)
        )
        df = df[mask][required].copy()
        df.columns = ["datahora", "sensor", "valor"]
        df["datahora"] = pd.to_datetime(df["datahora"], dayfirst=True, errors="coerce")
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        df = df.dropna(subset=["datahora"])
        return df.sort_values(["sensor", "datahora"]).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def _load_syos_benchmark_source(source) -> pd.DataFrame:
    """Aceita Path ou arquivo enviado pelo Streamlit para o benchmark SyOS."""
    if source is None:
        return pd.DataFrame()

    if isinstance(source, Path):
        return _load_syos_benchmark(source)

    source_name = str(getattr(source, "name", "")).lower()
    try:
        if source_name.endswith(".csv"):
            df = pd.read_csv(source)
        else:
            df = pd.read_excel(source)

        required = ["Data", "Apelido do balcão", "Temperatura"]
        if not all(col in df.columns for col in required):
            return pd.DataFrame()
        mask = (
            df["Apelido do balcão"].astype(str).str.contains("Balcão", case=False, na=False)
            & ~df["Apelido do balcão"].astype(str).str.contains("Câmara", case=False, na=False)
        )
        df = df.loc[mask, required].copy()
        df.columns = ["datahora", "sensor", "valor"]
        df["datahora"] = pd.to_datetime(df["datahora"], dayfirst=True, errors="coerce")
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        return df.dropna(subset=["datahora"]).sort_values(["sensor", "datahora"]).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def _normalize_to_5min_grid(df: pd.DataFrame, period_start, period_end) -> pd.DataFrame:
    """Reamostra a série para 5 minutos e completa lacunas com NaN."""
    return build_time_grid_from_raw(df, period_start, period_end, freq=BENCHMARK_GRID_FREQ)


def _compute_bm_hourly_profile(grid_df: pd.DataFrame, label: str) -> pd.DataFrame:
    """Calcula perfil horário de desconexão a partir do grid de 5 min."""
    df = grid_df.copy()
    df["hora"] = df["datahora"].dt.hour
    df["dia_ref"] = df["datahora"].dt.normalize()
    df["com_dado"] = df["valor"].notna().astype(int)
    profile = df.groupby("hora", as_index=False).agg(
        dias_amostrados=("dia_ref", "nunique"),
        total_slots=("com_dado", "size"),
        slots_com_dado=("com_dado", "sum"),
    )
    profile["minutos_esperados"] = profile["total_slots"] * 5
    profile["minutos_com_dado"] = profile["slots_com_dado"] * 5
    profile["minutos_sem_dado"] = profile["minutos_esperados"] - profile["minutos_com_dado"]
    profile["pct_conectado"] = (profile["minutos_com_dado"] / profile["minutos_esperados"] * 100).round(2)
    profile["pct_sem_dado"] = (profile["minutos_sem_dado"] / profile["minutos_esperados"] * 100).round(2)
    profile["min_sem_dado_por_dia"] = profile["minutos_sem_dado"] / profile["dias_amostrados"]
    profile["hora_label"] = profile["hora"].map(hour_label)
    profile["faixa"] = profile["hora"].apply(
        lambda h: "Desconexões entre 20h e 08h" if h in CRITICAL_HOURS else "Desconexões entre 08h e 20h"
    )
    profile["cenario"] = label
    return profile


def _compute_bm_events(grid_df: pd.DataFrame, min_gap_minutes: int = 20) -> pd.DataFrame:
    """Detecta eventos de desconexão (lacunas > min_gap_minutes) no grid."""
    return extract_disconnect_events(
        grid_df,
        min_gap_minutes=min_gap_minutes,
        slot_minutes=5,
    )


def _compute_bm_sensor_summary(grid_df, events_df, sensor_name, label):
    """Calcula resumo de desconexão para um sensor no benchmark."""
    df = grid_df.copy()
    df["dia"] = df["datahora"].dt.normalize()
    df["com_dado"] = df["valor"].notna().astype(int)
    daily = df.groupby("dia", as_index=False).agg(
        total_slots=("com_dado", "size"),
        slots_com_dado=("com_dado", "sum"),
    )
    daily["total_min"] = daily["total_slots"] * 5
    daily["min_com_dado"] = daily["slots_com_dado"] * 5
    daily["min_sem_dado"] = daily["total_min"] - daily["min_com_dado"]
    total_esp = daily["total_min"].sum()
    total_com = daily["min_com_dado"].sum()
    total_sem = daily["min_sem_dado"].sum()
    dias = len(daily)
    pct = (total_com / total_esp * 100) if total_esp > 0 else 0
    n_ev = len(events_df) if not events_df.empty else 0
    dur_ev = events_df["duracao_min"].sum() if not events_df.empty else 0
    max_ev = events_df["duracao_min"].max() if not events_df.empty else 0
    crit_ev = int(events_df["periodo_critico"].sum()) if not events_df.empty and "periodo_critico" in events_df.columns else 0
    return {
        "sensor": sensor_name,
        "cenario": label,
        "dias_amostrados": dias,
        "total_minutos_esperados": total_esp,
        "minutos_com_dado": total_com,
        "minutos_sem_dado": total_sem,
        "percentual_conectado": round(pct, 2),
        "percentual_desconectado": round(100 - pct, 2),
        "desconexoes_gt_20min": n_ev,
        "desconexoes_criticas": crit_ev,
        "tempo_total_desconectado_min": dur_ev,
        "maior_evento_min": max_ev,
        "min_sem_dado_por_dia": round(total_sem / dias, 2) if dias > 0 else 0,
        "desconexoes_por_dia": round(n_ev / dias, 2) if dias > 0 else 0,
        "tempo_desconectado_por_dia": round(dur_ev / dias, 2) if dias > 0 else 0,
    }


def build_sensor_comparison(
    before_summary: pd.DataFrame,
    after_summary: pd.DataFrame,
    before_hourly: pd.DataFrame,
    after_hourly: pd.DataFrame,
) -> pd.DataFrame:
    # Colunas que devem estar presentes
    required_columns = [
        "sensor",
        "percentual_conectado",
        "min_sem_dado_por_dia",
        "desconexoes_por_dia",
        "tempo_desconectado_por_dia",
        "maior_evento_min",
    ]

    # Verifica quais colunas existem em cada DataFrame
    before_cols = [col for col in required_columns if col in before_summary.columns]
    after_cols = [col for col in required_columns if col in after_summary.columns]

    # Se alguma coluna não existir, preenche com 0
    for col in required_columns:
        if col != "sensor":
            if col not in before_summary.columns:
                before_summary = before_summary.copy()
                before_summary[col] = 0.0
            if col not in after_summary.columns:
                after_summary = after_summary.copy()
                after_summary[col] = 0.0

    before_base = before_summary[required_columns].rename(
        columns={
            "percentual_conectado": "antes_pct_conectado",
            "min_sem_dado_por_dia": "antes_min_sem_dado_dia",
            "desconexoes_por_dia": "antes_desconexoes_dia",
            "tempo_desconectado_por_dia": "antes_tempo_desconectado_dia",
            "maior_evento_min": "antes_maior_evento_min",
        }
    )

    after_base = after_summary[required_columns].rename(
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


def render_main() -> None:
    """Função principal do dashboard."""
    st.set_page_config(page_title="Comparativo antes x depois", layout="wide")
    render_dashboard()


def render_dashboard() -> None:
    # Não chamar st.set_page_config aqui - já foi feito em render_main()

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

    st.title("Comparação de desconexões antes e depois da troca do gateway")
    st.caption(
        "Os períodos são diferentes. Por isso, os gráficos principais usam hora do dia "
        "e métricas normalizadas por dia amostrado. Visão geral, Por sensor e Eventos "
        "são recalculados a partir dos dados brutos em grade de 1 minuto; a aba SyOS vs Sagil "
        "usa alinhamento em 5 minutos apenas para a comparação justa com a SyOS."
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
    # Calcula dias SELECIONADOS (não apenas dias com dados)
    after_sample_days = (
        pd.Timestamp(after_end_date) - pd.Timestamp(after_start_date)
    ).days + 1

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
        icon: str = "",
        invert: bool = False,
        delta_suffix: str = "",
        subtitle: str = "",
    ) -> None:
        delta = after_val - before_val
        abs_delta = abs(delta)

        if unit == "%":
            after_fmt = f"{fmt_num(after_val)}%"
            before_fmt = f"{fmt_num(before_val)}%"
            delta_text = f"{abs_delta:.2f} p.p."
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
            "Percentual de conectividade geral",
            before_metrics["pct_conectado"],
            after_metrics["pct_conectado"],
            "%",
           
            
        )
    with kpi_2:
        _kpi_card(
            "Tempo offline por sensor (dia)",
            before_metrics["min_sem_dado_por_dia_por_sensor"],
            after_metrics["min_sem_dado_por_dia_por_sensor"],
            "duration",
          
            invert=True,
            delta_suffix="/dia",
        )
    with kpi_3:
        _kpi_card(
            "Média de quedas (>20min) por dia",
            before_metrics["desconexoes_por_dia"],
            after_metrics["desconexoes_por_dia"],
            "",
           
            invert=True,
            delta_suffix=" quedas/dia",
        )
    with kpi_4:
        _kpi_card(
            "Percentual de tempo offline entre 20h e 8h",
            before_metrics["pct_sem_dado_critico"],
            after_metrics["pct_sem_dado_critico"],
            "%",
            
            invert=True,
            
        )

    tab_geral, tab_sensor, tab_eventos, tab_benchmark = st.tabs(
        ["Visão geral", "Por sensor", "Eventos", "SyOS vs Sagil"]
    )

    with tab_geral:
        st.caption(
            "Como a disponibilidade de dados variou entre o período anterior e o atual, hora a hora, "
            "a partir dos dados brutos reconstruídos em 1 minuto."
        )
        left, right = st.columns(2)

        with left:
            overall_sorted = overall_hourly.sort_values(["cenario", "hora"])
            overall_fig = go.Figure()
            for cenario_name, color in [("Antes", "#eab308"), ("Depois", "#0284c7")]:
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
                        #"Dias amostrados: %{customdata[2]}"
                        "<extra></extra>"
                    ),
                ))
            update_hour_axis(overall_fig)
            overall_fig.update_layout(
                title="Percentual sem dado por hora do dia",
                xaxis_title="Hora do dia",
                yaxis_title="Percentual sem dado",
                legend_title="",
                template=PLOTLY_TEMPLATE,
                hovermode="x unified",
                hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
            )
            st.plotly_chart(overall_fig, use_container_width=True)

        with right:
            band_summary["pct_fmt"] = band_summary["pct_sem_dado"].apply(lambda x: f"{x:.2f}%")
            faixa_chart = px.bar(
                band_summary,
                x="faixa",
                y="pct_sem_dado",
                color="cenario",
                barmode="group",
                title="Percentual sem dado: 20h às 8h vs 8h às 20h",
                template=PLOTLY_TEMPLATE,
                text="pct_fmt",
                custom_data=["pct_fmt"],
                color_discrete_map={"Antes": "#eab308", "Depois": "#0284c7"},
            )
            faixa_chart.update_traces(
                textposition="outside",
                textfont=dict(size=11),
                hovertemplate="<b>%{fullData.name}</b><br>Faixa: %{x}<br>Sem dado: %{customdata[0]}<extra></extra>"
            )
            faixa_chart.update_layout(
                xaxis_title="Faixa horária",
                yaxis_title="Percentual sem dado",
                legend_title="",
                hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
            )
            st.plotly_chart(faixa_chart, use_container_width=True)

        bottom_left, bottom_right = st.columns(2)

        with bottom_left:
            summary_sorted_ev = summary_long.sort_values(["sensor", "cenario"]).copy()
            summary_sorted_ev["desc_fmt"] = summary_sorted_ev["desconexoes_por_dia"].apply(lambda x: f"{x:.2f}" if x > 0 else "0")
            eventos_sensor_chart = px.bar(
                summary_sorted_ev,
                x="sensor",
                y="desconexoes_por_dia",
                color="cenario",
                barmode="group",
                title="Desconexões (>20 min) por dia — por sensor",
                template=PLOTLY_TEMPLATE,
                text="desc_fmt",
                custom_data=["desc_fmt"],
                color_discrete_map={"Antes": "#eab308", "Depois": "#0284c7"},
            )
            eventos_sensor_chart.update_traces(
                textposition="outside",
                textfont=dict(size=11),
                hovertemplate="<b>%{fullData.name}</b><br>Sensor: %{x}<br>Desconexões/dia: %{customdata[0]}<extra></extra>"
            )
            eventos_sensor_chart.update_layout(
                xaxis_title="Sensor",
                yaxis_title="Desconexões / dia",
                legend_title="",
                hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
            )
            st.plotly_chart(eventos_sensor_chart, use_container_width=True)

        with bottom_right:
            summary_sorted = summary_long.sort_values(["sensor", "cenario"]).copy()
            summary_sorted["horas_sem_dado_por_dia"] = summary_sorted["min_sem_dado_por_dia"] / 60
            summary_sorted["duracao_fmt"] = summary_sorted["min_sem_dado_por_dia"].apply(fmt_duration)

            missing_sensor_chart = px.bar(
                summary_sorted,
                x="sensor",
                y="horas_sem_dado_por_dia",
                color="cenario",
                barmode="group",
                title="Tempo sem dado por dia — por sensor",
                template=PLOTLY_TEMPLATE,
                text="duracao_fmt",
                custom_data=["duracao_fmt"],
                color_discrete_map={"Antes": "#eab308", "Depois": "#0284c7"},
            )
            missing_sensor_chart.update_traces(
                textposition="outside",
                textfont=dict(size=11),
                hovertemplate="<b>%{fullData.name}</b><br>Sensor: %{x}<br>Tempo sem dado/dia: %{customdata[0]}<extra></extra>"
            )
            missing_sensor_chart.update_layout(
                xaxis_title="Sensor",
                yaxis_title="Horas (escala temporal)",
                legend_title="",
                hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
            )
            st.plotly_chart(missing_sensor_chart, use_container_width=True)

        # Tabela comparison_display removida a pedido do usuário

    with tab_sensor:
        st.caption(
            "Análise detalhada por sensor: percentual sem dado hora a hora em grade de 1 minuto "
            "e leituras brutas mostrando as lacunas reais de conexão."
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
                    for cenario_name, color in [("Antes", "#eab308"), ("Depois", "#0284c7")]:
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
                                #"Dias amostrados: %{customdata[2]}"
                                "<extra></extra>"
                            ),
                        ))
                    update_hour_axis(fig_s)
                    fig_s.update_layout(
                        title=s_name,
                        xaxis_title="Hora do dia",
                        yaxis_title="Percentual sem dado",
                        legend_title="",
                        height=350,
                        template=PLOTLY_TEMPLATE,
                        hovermode="x unified",
                        hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
                    )
                    st.plotly_chart(fig_s, use_container_width=True)

        # ── Gráfico de leituras brutas (timeline) ──
        st.markdown("---")
        st.subheader("📡 Leituras brutas — visualização de lacunas")
        st.caption(
            "Cada ponto é uma leitura bruta real do sensor, com timestamp original. "
            "Onde a linha desaparece, o sensor ficou sem enviar dados."
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
                            line=dict(width=1, color="#eab308"),
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
                            line=dict(width=1, color="#0284c7"),
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
            st.info("Não houve eventos de desconexão acima de 20 minutos nos filtros atuais.")
        else:
            st.caption(
                "Cada evento é um período contínuo sem dados do sensor, reconstruído em grade de 1 minuto "
                "a partir dos dados brutos, com duração acima de 20 minutos."
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
                            <span style="color:#eab308;font-weight:700;">Antes: {b_count}</span>
                            <span style="color:#ccc;"> | </span>
                            <span style="color:#0284c7;font-weight:700;">Depois: {a_count}</span>
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
                            <span style="color:#eab308;font-weight:700;">Antes: {fmt_duration(b_med)}</span>
                            <span style="color:#ccc;"> | </span>
                            <span style="color:#0284c7;font-weight:700;">Depois: {fmt_duration(a_med)}</span>
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
                            <span style="color:#eab308;font-weight:700;">Antes: {fmt_duration(b_max)}</span>
                            <span style="color:#ccc;"> | </span>
                            <span style="color:#0284c7;font-weight:700;">Depois: {fmt_duration(a_max)}</span>
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
                            <span style="color:#eab308;font-weight:700;">Antes: {int(b_crit)}</span>
                            <span style="color:#ccc;"> | </span>
                            <span style="color:#0284c7;font-weight:700;">Depois: {int(a_crit)}</span>
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
                    color_discrete_map={"Antes": "#eab308", "Depois": "#0284c7"},
                    text="eventos",
                )
                fig_sensor_ev.update_traces(
                    textposition="outside",
                    textfont=dict(size=11),
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
                sensor_dur["tempo_total_horas"] = sensor_dur["tempo_total"] / 60
                sensor_dur["duracao_fmt"] = sensor_dur["tempo_total"].apply(fmt_duration)
                fig_sensor_dur = px.bar(
                    sensor_dur,
                    x="sensor",
                    y="tempo_total_horas",
                    color="cenario",
                    barmode="group",
                    title="Tempo total offline por sensor",
                    template=PLOTLY_TEMPLATE,
                    color_discrete_map={"Antes": "#eab308", "Depois": "#0284c7"},
                    text="duracao_fmt",
                    custom_data=["duracao_fmt"],
                )
                fig_sensor_dur.update_traces(
                    textposition="outside",
                    textfont=dict(size=11),
                    hovertemplate="<b>%{fullData.name}</b><br>Sensor: %{x}<br>Tempo offline: %{customdata[0]}<extra></extra>"
                )
                fig_sensor_dur.update_layout(
                    xaxis_title="Sensor",
                    yaxis_title="Horas (escala temporal)",
                    legend_title="",
                    hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
                )
                st.plotly_chart(fig_sensor_dur, use_container_width=True)

            st.markdown("---")
            st.subheader("Linha do tempo dos eventos")
            st.caption(
                "Cada barra representa um evento reconstruído a partir dos dados brutos. "
                "O comprimento indica a duração real do período sem leitura."
            )

            timeline_data = events_long.copy()
            if {"inicio_desconexao", "fim_desconexao"}.issubset(timeline_data.columns):
                timeline_data = timeline_data.dropna(subset=["inicio_desconexao", "fim_desconexao"])
                for cenario_name, cenario_color, cenario_label in [
                    ("Antes", "#eab308", f"Antes — {before_filtered['period_label']}"),
                    ("Depois", "#0284c7", f"Depois — {after_filtered['period_label']}"),
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
            required_event_cols = {
                "cenario",
                "sensor",
                "inicio_desconexao",
                "fim_desconexao",
                "duracao_min",
                "periodo_critico",
            }
            if not events_long.empty and required_event_cols.issubset(
                events_long.columns
            ):
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
                events_view["duracao_fmt"] = events_view["duracao_min"].apply(
                    fmt_duration
                )
                events_view["inicio_desconexao"] = events_view[
                    "inicio_desconexao"
                ].dt.strftime("%d/%m/%Y %H:%M")
                events_view["fim_desconexao"] = events_view[
                    "fim_desconexao"
                ].dt.strftime("%d/%m/%Y %H:%M")
                events_view["periodo_critico"] = events_view["periodo_critico"].map(
                    {True: "⚠️ Sim", False: "Não"}
                )
                events_view = events_view.sort_values(
                    ["cenario", "duracao_min"], ascending=[True, False]
                )
                events_view = events_view.drop(columns=["duracao_min"])
                events_view = events_view.rename(
                    columns={
                        "cenario": "Cenário",
                        "sensor": "Sensor",
                        "inicio_desconexao": "Início",
                        "fim_desconexao": "Fim",
                        "duracao_fmt": "Duração",
                        "periodo_critico": "Horário Crítico? (entre 20h e 8h)",
                    }
                )
                st.dataframe(events_view, use_container_width=True, hide_index=True)
            else:
                st.caption("Nenhum evento para exibir.")

    with tab_benchmark:
        repo_syos_antes = _load_syos_benchmark_source(COMPETITOR_DIR / COMPETITOR_BEFORE_FILE)
        repo_syos_depois = _load_syos_benchmark_source(COMPETITOR_DIR / COMPETITOR_AFTER_FILE)
        syos_antes_upload = None
        syos_depois_upload = None

        st.subheader("Desconexões Sagil x SyOS")
        st.caption(
            "Comparação de desconexões entre sensores Sagil e SyOS nos mesmos períodos. "
            "A Sagil é alinhada em 5 minutos apenas nesta aba para comparação justa com a SyOS."
        )

        if repo_syos_antes.empty or repo_syos_depois.empty:
            st.info(
                "Os arquivos da SyOS não estão disponíveis no servidor do Streamlit Cloud. "
                "Você pode enviar os Excels originais abaixo para habilitar esta aba nesta sessão."
            )
            upload_col1, upload_col2 = st.columns(2)
            with upload_col1:
                syos_antes_upload = st.file_uploader(
                    "SyOS ANTES (.xlsx/.xls)",
                    type=["xlsx", "xls"],
                    key="syos_antes_upload",
                    help="Arquivo original da SyOS para o período ANTES.",
                )
            with upload_col2:
                syos_depois_upload = st.file_uploader(
                    "SyOS DEPOIS (.xlsx/.xls)",
                    type=["xlsx", "xls"],
                    key="syos_depois_upload",
                    help="Arquivo original da SyOS para o período DEPOIS.",
                )

        # ── Carrega dados brutos ──
        sagil_antes_raw = _load_sagil_benchmark("antes")
        sagil_depois_raw = _load_sagil_benchmark("depois")
        syos_antes_raw = (
            repo_syos_antes if not repo_syos_antes.empty else _load_syos_benchmark_source(syos_antes_upload)
        )
        syos_depois_raw = (
            repo_syos_depois if not repo_syos_depois.empty else _load_syos_benchmark_source(syos_depois_upload)
        )

        data_ok = all(
            not d.empty for d in [sagil_antes_raw, sagil_depois_raw, syos_antes_raw, syos_depois_raw]
        )
        if not data_ok:
            st.error("⚠️ Não foi possível carregar todos os dados do benchmark.")
            missing = []
            if sagil_antes_raw.empty:
                missing.append("Sagil ANTES")
            if sagil_depois_raw.empty:
                missing.append("Sagil DEPOIS")
            if syos_antes_raw.empty:
                missing.append("SyOS ANTES")
            if syos_depois_raw.empty:
                missing.append("SyOS DEPOIS")
            st.warning(f"Dados faltando: {', '.join(missing)}")
            if "SyOS ANTES" in missing or "SyOS DEPOIS" in missing:
                st.caption(
                    "Para o deploy, envie os dois arquivos SyOS acima ou versione-os na pasta `syos/` do repositório."
                )
        else:
            # ── KPI card para benchmark ──
            def _bm_kpi_card(
                title: str,
                sagil_val: float,
                syos_val: float,
                unit: str,
                invert: bool = False,
                delta_suffix: str = "",
            ) -> None:
                delta = sagil_val - syos_val
                abs_delta = abs(delta)

                if unit == "%":
                    sagil_fmt = f"{fmt_num(sagil_val)}%"
                    syos_fmt = f"{fmt_num(syos_val)}%"
                    delta_text = f"{abs_delta:.2f} p.p."
                elif unit == "duration":
                    sagil_fmt = fmt_duration(sagil_val)
                    syos_fmt = fmt_duration(syos_val)
                    delta_text = fmt_duration(abs_delta)
                else:
                    sagil_fmt = fmt_num(sagil_val)
                    syos_fmt = fmt_num(syos_val)
                    delta_text = fmt_num(abs_delta)

                if delta_suffix:
                    delta_text += delta_suffix

                improved = delta > 0
                if invert:
                    improved = not improved

                if abs(delta) < 0.01:
                    badge_color = "#6c757d"
                    badge_bg = "#f0f0f0"
                    badge_text = "sem diferença"
                elif improved:
                    badge_color = "#1a7f37"
                    badge_bg = "#dafbe1"
                    badge_text = f"Sagil melhor em {delta_text}"
                else:
                    badge_color = "#cf222e"
                    badge_bg = "#ffebe9"
                    badge_text = f"SyOS melhor em {delta_text}"

                icon_bg = badge_bg

                st.markdown(
                    f"""
                    <div style="background:#fff;border:1px solid #e8e8e8;border-radius:12px;padding:18px 16px;">
                        <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;">
                            <span style="font-size:0.82em;color:#666;font-weight:500;">{title}</span>
                        </div>
                        <div style="display:flex;justify-content:space-between;align-items:baseline;">
                            <div>
                                <div style="font-size:0.7em;color:#0284c7;font-weight:600;">Sagil</div>
                                <div style="font-size:1.6em;font-weight:700;color:#0284c7;line-height:1.1;">{sagil_fmt}</div>
                            </div>
                            <div style="font-size:1.2em;color:#ccc;">vs</div>
                            <div style="text-align:right;">
                                <div style="font-size:0.7em;color:#14b8a6;font-weight:600;">SyOS</div>
                                <div style="font-size:1.6em;font-weight:700;color:#14b8a6;line-height:1.1;">{syos_fmt}</div>
                            </div>
                        </div>
                        <div style="margin-top:10px;">
                            <span style="background:{badge_bg};color:{badge_color};font-size:0.73em;font-weight:600;padding:3px 8px;border-radius:6px;">
                                {badge_text}
                            </span>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            # ── Processa cada período ──
            for period_key, period_name in [("antes", "ANTES"), ("depois", "DEPOIS")]:
                bp = BENCHMARK_PERIODS[period_key]
                p_start, p_end, p_label = bp["start"], bp["end"], bp["label"]

                sagil_raw = sagil_antes_raw if period_key == "antes" else sagil_depois_raw
                syos_raw = syos_antes_raw if period_key == "antes" else syos_depois_raw

                st.markdown("---")
                st.subheader(f"Período de amostragem: {period_name} — {p_label}")

                # ── Processa cada par de sensores ──
                all_sagil_summaries = []
                all_syos_summaries = []
                all_sagil_hourly = []
                all_syos_hourly = []
                all_sagil_grids = {}
                all_syos_grids = {}

                for pair in BENCHMARK_SENSOR_PAIRS:
                    sg_sensor = sagil_raw[sagil_raw["sensor"] == pair["sagil"]]
                    sy_sensor = syos_raw[syos_raw["sensor"] == pair["syos"]]

                    sg_grid = _normalize_to_5min_grid(sg_sensor, p_start, p_end)
                    sy_grid = _normalize_to_5min_grid(sy_sensor, p_start, p_end)
                    all_sagil_grids[pair["titulo"]] = sg_grid
                    all_syos_grids[pair["titulo"]] = sy_grid

                    sg_events = _compute_bm_events(sg_grid)
                    sy_events = _compute_bm_events(sy_grid)

                    sg_summ = _compute_bm_sensor_summary(sg_grid, sg_events, pair["titulo"], "Sagil")
                    sy_summ = _compute_bm_sensor_summary(sy_grid, sy_events, pair["titulo"], "SyOS")
                    all_sagil_summaries.append(sg_summ)
                    all_syos_summaries.append(sy_summ)

                    sg_hp = _compute_bm_hourly_profile(sg_grid, "Sagil")
                    sy_hp = _compute_bm_hourly_profile(sy_grid, "SyOS")
                    sg_hp["sensor"] = pair["titulo"]
                    sy_hp["sensor"] = pair["titulo"]
                    all_sagil_hourly.append(sg_hp)
                    all_syos_hourly.append(sy_hp)

                sagil_summary_df = pd.DataFrame(all_sagil_summaries)
                syos_summary_df = pd.DataFrame(all_syos_summaries)
                summary_long = pd.concat([sagil_summary_df, syos_summary_df], ignore_index=True)

                hourly_all = pd.concat(all_sagil_hourly + all_syos_hourly, ignore_index=True)

                # ── Overall hourly (agrega todos os sensores) ──
                overall_hourly = hourly_all.groupby(["cenario", "hora"], as_index=False).agg(
                    minutos_esperados=("minutos_esperados", "sum"),
                    minutos_com_dado=("minutos_com_dado", "sum"),
                    minutos_sem_dado=("minutos_sem_dado", "sum"),
                    dias_amostrados=("dias_amostrados", "first"),
                )
                overall_hourly["pct_sem_dado"] = (
                    overall_hourly["minutos_sem_dado"] / overall_hourly["minutos_esperados"] * 100
                ).round(2)
                overall_hourly["min_sem_dado_por_dia"] = (
                    overall_hourly["minutos_sem_dado"] / overall_hourly["dias_amostrados"]
                )

                # ── Overall KPIs ──
                num_sensors = len(BENCHMARK_SENSOR_PAIRS)
                sg_dias = sagil_summary_df["dias_amostrados"].iloc[0] if not sagil_summary_df.empty else 1
                sy_dias = syos_summary_df["dias_amostrados"].iloc[0] if not syos_summary_df.empty else 1

                sg_total_esp = sagil_summary_df["total_minutos_esperados"].sum()
                sg_total_com = sagil_summary_df["minutos_com_dado"].sum()
                sg_pct = (sg_total_com / sg_total_esp * 100) if sg_total_esp > 0 else 0
                sg_offline_dia = sagil_summary_df["minutos_sem_dado"].sum() / sg_dias / num_sensors
                sg_desc_dia = sagil_summary_df["desconexoes_gt_20min"].sum() / sg_dias

                sy_total_esp = syos_summary_df["total_minutos_esperados"].sum()
                sy_total_com = syos_summary_df["minutos_com_dado"].sum()
                sy_pct = (sy_total_com / sy_total_esp * 100) if sy_total_esp > 0 else 0
                sy_offline_dia = syos_summary_df["minutos_sem_dado"].sum() / sy_dias / num_sensors
                sy_desc_dia = syos_summary_df["desconexoes_gt_20min"].sum() / sy_dias

                # Período crítico
                sg_crit_hp = pd.concat(all_sagil_hourly)
                sg_crit = sg_crit_hp[sg_crit_hp["hora"].isin(CRITICAL_HOURS)]
                sg_crit_pct = (sg_crit["minutos_sem_dado"].sum() / sg_crit["minutos_esperados"].sum() * 100) if not sg_crit.empty and sg_crit["minutos_esperados"].sum() > 0 else 0

                sy_crit_hp = pd.concat(all_syos_hourly)
                sy_crit = sy_crit_hp[sy_crit_hp["hora"].isin(CRITICAL_HOURS)]
                sy_crit_pct = (sy_crit["minutos_sem_dado"].sum() / sy_crit["minutos_esperados"].sum() * 100) if not sy_crit.empty and sy_crit["minutos_esperados"].sum() > 0 else 0

                # ── Info dos sistemas ──
                info_left, info_right = st.columns(2)
                info_left.markdown(
                    f"**Sagil**  \n"
                    f"Sensores analisados: {num_sensors}  \n"
                    #f"Dias amostrados: {sg_dias}"
                )
                info_right.markdown(
                    f"**SyOS**  \n"
                    f"Sensores analisados: {num_sensors}  \n"
                    #f"Dias amostrados: {sy_dias}"
                )

                # ── KPI Cards ──
                kpi_1, kpi_2, kpi_3, kpi_4 = st.columns(4)
                with kpi_1:
                    _bm_kpi_card("Conectividade geral", sg_pct, sy_pct, "%")
                with kpi_2:
                    _bm_kpi_card("Tempo offline por sensor (dia)", sg_offline_dia, sy_offline_dia, "duration", invert=True, delta_suffix="/dia")
                with kpi_3:
                    _bm_kpi_card("Média de quedas (>20min) por dia", sg_desc_dia, sy_desc_dia, "", invert=True, delta_suffix=" quedas/dia")
                with kpi_4:
                    _bm_kpi_card("Percentual de tempo offline entre 20h e 08h", sg_crit_pct, sy_crit_pct, "%", invert=True)

                # ── Gráficos (layout igual Visão Geral) ──
                left, right = st.columns(2)

                with left:
                    overall_sorted = overall_hourly.sort_values(["cenario", "hora"])
                    fig_h = go.Figure()
                    for cen_name, color in [("Sagil", "#0284c7"), ("SyOS", "#14b8a6")]:
                        cd = overall_sorted[overall_sorted["cenario"] == cen_name]
                        if cd.empty:
                            continue
                        fig_h.add_trace(go.Scatter(
                            x=cd["hora"],
                            y=cd["pct_sem_dado"],
                            mode="lines+markers",
                            name=cen_name,
                            line=dict(color=color),
                            marker=dict(size=6),
                            customdata=list(zip(
                                cd["minutos_sem_dado"].apply(fmt_duration),
                                cd["minutos_esperados"].apply(fmt_duration),
                                cd["min_sem_dado_por_dia"].apply(fmt_duration),
                            )),
                            hovertemplate=(
                                "<b>%{fullData.name}</b><br>"
                                "Hora: %{x}:00<br>"
                                "Percentual sem dado: %{y:.1f}%<br>"
                                "Total sem dado: %{customdata[0]} de %{customdata[1]}<br>"
                                "Média por dia: %{customdata[2]}"
                                "<extra></extra>"
                            ),
                        ))
                    update_hour_axis(fig_h)
                    fig_h.update_layout(
                        title="Percentual sem dado por hora do dia",
                        xaxis_title="Hora do dia",
                        yaxis_title="Percentual sem dado",
                        legend_title="",
                        template=PLOTLY_TEMPLATE,
                        hovermode="x unified",
                        hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
                    )
                    st.plotly_chart(fig_h, use_container_width=True)

                with right:
                    band_data = hourly_all.copy()
                    band_summary_bm = band_data.groupby(["cenario", "faixa"], as_index=False).agg(
                        minutos_esperados=("minutos_esperados", "sum"),
                        minutos_sem_dado=("minutos_sem_dado", "sum"),
                    )
                    band_summary_bm["pct_sem_dado"] = (
                        band_summary_bm["minutos_sem_dado"] / band_summary_bm["minutos_esperados"] * 100
                    ).round(2)
                    band_summary_bm["pct_fmt"] = band_summary_bm["pct_sem_dado"].apply(lambda x: f"{x:.2f}%")

                    fig_band = px.bar(
                        band_summary_bm,
                        x="faixa",
                        y="pct_sem_dado",
                        color="cenario",
                        barmode="group",
                        title="Percentual sem dado: 20h às 8h vs 8h às 20h",
                        template=PLOTLY_TEMPLATE,
                        text="pct_fmt",
                        color_discrete_map={"Sagil": "#0284c7", "SyOS": "#14b8a6"},
                    )
                    fig_band.update_traces(
                        textposition="outside",
                        textfont=dict(size=11),
                        hovertemplate="<b>%{fullData.name}</b><br>Faixa: %{x}<br>Sem dado: %{text}<extra></extra>",
                    )
                    fig_band.update_layout(
                        xaxis_title="Faixa horária",
                        yaxis_title="Percentual sem dado",
                        legend_title="",
                        hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
                    )
                    st.plotly_chart(fig_band, use_container_width=True)

                # ── Barras por sensor ──
                bottom_left, bottom_right = st.columns(2)

                with bottom_left:
                    sl_sorted = summary_long.sort_values(["sensor", "cenario"]).copy()
                    sl_sorted["desc_fmt"] = sl_sorted["desconexoes_por_dia"].apply(
                        lambda x: f"{x:.2f} /dia" if x > 0 else "0"
                    )
                    fig_desc = px.bar(
                        sl_sorted,
                        x="sensor",
                        y="desconexoes_por_dia",
                        color="cenario",
                        barmode="group",
                        title="Desconexões (>20 min) por dia — por sensor",
                        template=PLOTLY_TEMPLATE,
                        text="desc_fmt",
                        color_discrete_map={"Sagil": "#0284c7", "SyOS": "#14b8a6"},
                    )
                    fig_desc.update_traces(
                        textposition="outside",
                        textfont=dict(size=11),
                        hovertemplate="<b>%{fullData.name}</b><br>Sensor: %{x}<br>Desconexões/dia: %{text}<extra></extra>",
                    )
                    fig_desc.update_layout(
                        xaxis_title="Sensor",
                        yaxis_title="Desconexões / dia",
                        legend_title="",
                        hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
                    )
                    st.plotly_chart(fig_desc, use_container_width=True)

                with bottom_right:
                    sl_sorted2 = summary_long.sort_values(["sensor", "cenario"]).copy()
                    sl_sorted2["horas_sem_dado_por_dia"] = sl_sorted2["min_sem_dado_por_dia"] / 60
                    sl_sorted2["duracao_fmt"] = sl_sorted2["min_sem_dado_por_dia"].apply(fmt_duration)

                    fig_off = px.bar(
                        sl_sorted2,
                        x="sensor",
                        y="horas_sem_dado_por_dia",
                        color="cenario",
                        barmode="group",
                        title="Tempo sem dado por dia — por sensor",
                        template=PLOTLY_TEMPLATE,
                        text="duracao_fmt",
                        color_discrete_map={"Sagil": "#0284c7", "SyOS": "#14b8a6"},
                    )
                    fig_off.update_traces(
                        textposition="outside",
                        textfont=dict(size=11),
                        hovertemplate="<b>%{fullData.name}</b><br>Sensor: %{x}<br>Tempo sem dado/dia: %{text}<extra></extra>",
                    )
                    fig_off.update_layout(
                        xaxis_title="Sensor",
                        yaxis_title="Horas (escala temporal)",
                        legend_title="",
                        hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial"),
                    )
                    st.plotly_chart(fig_off, use_container_width=True)

                # ── Leituras alinhadas em 5 min ──
                st.markdown("---")
                st.subheader(f"Leituras alinhadas em 5 min — {period_name}")
                st.caption(
                    "Cada ponto mostra a série alinhada em 5 minutos para comparação justa entre Sagil e SyOS."
                )

                for pair in BENCHMARK_SENSOR_PAIRS:
                    st.markdown(f"#### {pair['titulo']}")
                    col_sg, col_sy = st.columns(2)

                    sg_grid = all_sagil_grids[pair["titulo"]]
                    sy_grid = all_syos_grids[pair["titulo"]]

                    with col_sg:
                        sg_plot = sg_grid.dropna(subset=["valor"]).sort_values("datahora")
                        if sg_plot.empty:
                            st.info("Sem dados Sagil")
                        else:
                            fig_sg = go.Figure()
                            fig_sg.add_trace(go.Scatter(
                                x=sg_plot["datahora"],
                                y=sg_plot["valor"],
                                mode="lines",
                                line=dict(width=1, color="#0284c7"),
                                name="Sagil",
                                connectgaps=False,
                                hovertemplate="<b>Sagil</b><br>%{x|%d/%m %H:%M}<br>Leitura: %{y:.1f}<extra></extra>",
                            ))
                            fig_sg.update_layout(
                                title=f"Sagil ({pair['sagil']})",
                                template=PLOTLY_TEMPLATE,
                                height=280,
                                margin=dict(l=50, r=10, t=40, b=35),
                                xaxis_title="",
                                yaxis_title="Leitura",
                                xaxis=dict(tickformat="%d/%m\n%H:%M"),
                                hoverlabel=dict(bgcolor="white", font_size=12, font_family="Arial"),
                            )
                            st.plotly_chart(fig_sg, use_container_width=True)

                    with col_sy:
                        sy_plot = sy_grid.dropna(subset=["valor"]).sort_values("datahora")
                        if sy_plot.empty:
                            st.info("Sem dados SyOS")
                        else:
                            fig_sy = go.Figure()
                            fig_sy.add_trace(go.Scatter(
                                x=sy_plot["datahora"],
                                y=sy_plot["valor"],
                                mode="lines",
                                line=dict(width=1, color="#14b8a6"),
                                name="SyOS",
                                connectgaps=False,
                                hovertemplate="<b>SyOS</b><br>%{x|%d/%m %H:%M}<br>Leitura: %{y:.1f}<extra></extra>",
                            ))
                            fig_sy.update_layout(
                                title=f"SyOS ({pair['syos']})",
                                template=PLOTLY_TEMPLATE,
                                height=280,
                                margin=dict(l=50, r=10, t=40, b=35),
                                xaxis_title="",
                                yaxis_title="Leitura",
                                xaxis=dict(tickformat="%d/%m\n%H:%M"),
                                hoverlabel=dict(bgcolor="white", font_size=12, font_family="Arial"),
                            )
                            st.plotly_chart(fig_sy, use_container_width=True)


if __name__ == "__main__":
    if st.runtime.exists():
        render_main()
    else:
        print("Este arquivo e um app Streamlit.")
        print("Execute assim:")
        print("  streamlit run .\\dashboard_ubidots_comparativo.py")
