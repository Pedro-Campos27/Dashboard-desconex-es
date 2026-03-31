from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


DEFAULT_BASE_DIR = Path(__file__).resolve().parent / "saida_ubidots_analise"
PLOTLY_TEMPLATE = "plotly_white"

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


@st.cache_data(show_spinner=False)
def load_dashboard_data(base_dir_str: str) -> dict[str, pd.DataFrame]:
    base_dir = Path(base_dir_str)
    if not base_dir.exists():
        raise FileNotFoundError(f"Pasta nao encontrada: {base_dir}")

    data: dict[str, list[pd.DataFrame]] = {name: [] for name in FILE_MAP}
    inventory_rows: list[dict[str, object]] = []

    resumo_geral = maybe_read_csv(base_dir / "resumo_geral.csv")
    if not resumo_geral.empty:
        inventory_rows.append(
            {
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
            if file_path.exists():
                inventory_rows.append(
                    {
                        "sensor": sensor,
                        "tipo": key,
                        "linhas": len(frame),
                        "arquivo": str(file_path.resolve()),
                    }
                )
            if not frame.empty or file_path.exists():
                data[key].append(frame)

    combined = {
        key: pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        for key, frames in data.items()
    }

    return {
        "base_dir": pd.DataFrame([{"caminho": str(base_dir.resolve())}]),
        "resumo_geral": resumo_geral,
        "inventory": pd.DataFrame(inventory_rows),
        **combined,
    }


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
    mask = df[column].between(start, end)
    return df.loc[mask].copy()


def filter_events_range(
    df: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    start_col = "inicio_desconexao" if "inicio_desconexao" in df.columns else None
    end_col = "fim_desconexao" if "fim_desconexao" in df.columns else None

    if start_col and end_col:
        mask = (df[start_col] <= end) & (df[end_col] >= start)
        return df.loc[mask].copy()

    if end_col:
        return df.loc[df[end_col].between(start, end)].copy()

    return df.copy()


def build_sensor_summary(
    metricas_por_dia: pd.DataFrame,
    eventos: pd.DataFrame,
    dados_normalizados: pd.DataFrame,
    dados_brutos: pd.DataFrame,
) -> pd.DataFrame:
    if metricas_por_dia.empty:
        return pd.DataFrame()

    resumo = (
        metricas_por_dia.groupby("sensor", as_index=False)
        .agg(
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

    return resumo.sort_values(
        ["minutos_sem_dado", "desconexoes_gt_20min"], ascending=[False, False]
    ).reset_index(drop=True)


def prepare_summary_display(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return summary

    display = summary.copy()
    numeric_round_2 = [
        "percentual_conectado",
        "percentual_desconectado",
        "maior_evento_min",
        "media_evento_min",
        "temperatura_media",
        "temperatura_min",
        "temperatura_max",
    ]
    for column in numeric_round_2:
        if column in display.columns:
            display[column] = display[column].round(2)

    return display.rename(
        columns={
            "sensor": "sensor",
            "total_minutos_esperados": "min_esperados",
            "minutos_com_dado": "min_com_dado",
            "minutos_sem_dado": "min_sem_dado",
            "desconexoes_gt_20min": "desconexoes_gt_20min",
            "tempo_total_desconectado_min": "tempo_desconectado_min",
            "desconexoes_criticas": "desconexoes_criticas",
            "percentual_conectado": "pct_conectado",
            "percentual_desconectado": "pct_desconectado",
            "maior_evento_min": "maior_evento_min",
            "media_evento_min": "media_evento_min",
            "temperatura_media": "temp_media",
            "temperatura_min": "temp_min",
            "temperatura_max": "temp_max",
            "registros_brutos": "registros_brutos",
        }
    )


def build_period_label(metricas_por_dia: pd.DataFrame) -> str:
    if metricas_por_dia.empty or "dia" not in metricas_por_dia.columns:
        return "Periodo indisponivel"
    inicio = metricas_por_dia["dia"].min()
    fim = metricas_por_dia["dia"].max()
    if pd.isna(inicio) or pd.isna(fim):
        return "Periodo indisponivel"
    return f"{inicio.strftime('%d/%m/%Y')} a {fim.strftime('%d/%m/%Y')}"


def render_dashboard() -> None:
    st.set_page_config(page_title="Dashboard Ubidots", layout="wide")

    try:
        data = load_dashboard_data(str(DEFAULT_BASE_DIR))
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.stop()

    metricas_periodo = data["metricas_periodo"]
    metricas_por_dia = data["metricas_por_dia"]
    eventos = data["eventos"]
    dados_normalizados = data["dados_normalizados"]
    dados_brutos = data["dados_brutos"]
    resumo_geral = data["resumo_geral"]
    inventory = data["inventory"]

    sensores = sorted(metricas_por_dia["sensor"].dropna().unique().tolist())
    if not sensores:
        st.error("Nenhum sensor encontrado nos CSVs.")
        st.stop()

    datas_disponiveis = metricas_por_dia["dia"].dropna()
    data_inicial = datas_disponiveis.min().date()
    data_final = datas_disponiveis.max().date()

    st.title("Análise de Desconexões")
    st.caption(
        f"Base: {DEFAULT_BASE_DIR.resolve()} | Periodo encontrado: {build_period_label(metricas_por_dia)}"
    )

    with st.sidebar:
        st.header("Filtros")
        sensores_selecionados = st.multiselect(
            "Sensores",
            options=sensores,
            default=sensores,
        )
        intervalo = st.date_input(
            "Periodo",
            value=(data_inicial, data_final),
            min_value=data_inicial,
            max_value=data_final,
        )
        mostrar_bruto = st.checkbox("Sobrepor dados brutos", value=False)

    if not sensores_selecionados:
        st.warning("Selecione ao menos um sensor.")
        st.stop()

    if isinstance(intervalo, tuple) and len(intervalo) == 2:
        inicio_filtro, fim_filtro = intervalo
    else:
        inicio_filtro = fim_filtro = intervalo

    metricas_por_dia = metricas_por_dia[
        metricas_por_dia["sensor"].isin(sensores_selecionados)
    ].copy()
    metricas_periodo = metricas_periodo[
        metricas_periodo["sensor"].isin(sensores_selecionados)
    ].copy()
    eventos = eventos[eventos["sensor"].isin(sensores_selecionados)].copy()
    dados_normalizados = dados_normalizados[
        dados_normalizados["sensor"].isin(sensores_selecionados)
    ].copy()
    dados_brutos = dados_brutos[dados_brutos["sensor"].isin(sensores_selecionados)].copy()
    if not resumo_geral.empty and "sensor" in resumo_geral.columns:
        resumo_geral = resumo_geral[resumo_geral["sensor"].isin(sensores_selecionados)].copy()

    metricas_por_dia_filtradas = filter_date_range(
        metricas_por_dia, "dia", pd.Timestamp(inicio_filtro), pd.Timestamp(fim_filtro)
    )
    eventos_filtrados = filter_events_range(
        eventos, pd.Timestamp(inicio_filtro), pd.Timestamp(fim_filtro)
    )
    dados_normalizados_filtrados = filter_date_range(
        dados_normalizados, "datahora", pd.Timestamp(inicio_filtro), pd.Timestamp(fim_filtro)
    )
    dados_brutos_filtrados = filter_date_range(
        dados_brutos, "datahora", pd.Timestamp(inicio_filtro), pd.Timestamp(fim_filtro)
    )

    resumo_sensores = build_sensor_summary(
        metricas_por_dia_filtradas,
        eventos_filtrados,
        dados_normalizados_filtrados,
        dados_brutos_filtrados,
    )

    if resumo_sensores.empty:
        st.warning("Nao ha dados no intervalo selecionado.")
        st.stop()

    total_esperado = resumo_sensores["total_minutos_esperados"].sum()
    total_com_dado = resumo_sensores["minutos_com_dado"].sum()
    total_sem_dado = resumo_sensores["minutos_sem_dado"].sum()
    total_eventos = (
        int(eventos_filtrados["duracao_min"].notna().sum())
        if "duracao_min" in eventos_filtrados.columns
        else 0
    )
    conectividade = (total_com_dado / total_esperado * 100) if total_esperado else 0

    pior_sensor = resumo_sensores.iloc[0]

    card_1, card_2, card_3, card_4 = st.columns(4)
    card_1.metric("Sensores selecionados", fmt_int(len(sensores_selecionados)))
    card_2.metric("Conectividade ponderada", f"{fmt_num(conectividade)}%")
    card_3.metric("Minutos sem dado", fmt_int(total_sem_dado))
    card_4.metric("Desconexoes > 20 min", fmt_int(total_eventos))

    st.caption(
        f"Sensor mais critico no filtro atual: {pior_sensor['sensor']} "
        f"({fmt_int(pior_sensor['minutos_sem_dado'])} min sem dado)"
    )

    tab_resumo, tab_diario, tab_eventos, tab_series, tab_arquivos = st.tabs(
        ["Resumo", "Diario", "Eventos", "Series", "Arquivos"]
    )

    with tab_resumo:
        left, right = st.columns(2)

        with left:
            grafico_conectividade = px.bar(
                resumo_sensores.sort_values("percentual_conectado", ascending=True),
                x="percentual_conectado",
                y="sensor",
                orientation="h",
                title="% conectado por sensor",
                template=PLOTLY_TEMPLATE,
                text="percentual_conectado",
            )
            grafico_conectividade.update_traces(texttemplate="%{text:.2f}%")
            grafico_conectividade.update_layout(xaxis_title="% conectado", yaxis_title="")
            st.plotly_chart(grafico_conectividade, use_container_width=True)

        with right:
            grafico_desconectado = px.bar(
                resumo_sensores.sort_values("minutos_sem_dado", ascending=False),
                x="sensor",
                y="minutos_sem_dado",
                title="Minutos sem dado por sensor",
                template=PLOTLY_TEMPLATE,
                text="minutos_sem_dado",
            )
            grafico_desconectado.update_traces(texttemplate="%{text:.0f}")
            grafico_desconectado.update_layout(xaxis_title="", yaxis_title="min sem dado")
            st.plotly_chart(grafico_desconectado, use_container_width=True)

        st.dataframe(
            prepare_summary_display(resumo_sensores),
            use_container_width=True,
            hide_index=True,
        )

        if not metricas_periodo.empty:
            st.markdown("**Metricas de periodo originais**")
            st.dataframe(metricas_periodo, use_container_width=True, hide_index=True)

    with tab_diario:
        daily_left, daily_right = st.columns(2)

        with daily_left:
            grafico_diario_conectado = px.line(
                metricas_por_dia_filtradas.sort_values("dia"),
                x="dia",
                y="percentual_conectado",
                color="sensor",
                markers=True,
                title="% conectado por dia",
                template=PLOTLY_TEMPLATE,
            )
            grafico_diario_conectado.update_layout(
                xaxis_title="dia", yaxis_title="% conectado", legend_title=""
            )
            st.plotly_chart(grafico_diario_conectado, use_container_width=True)

        with daily_right:
            grafico_diario_gap = px.bar(
                metricas_por_dia_filtradas.sort_values("dia"),
                x="dia",
                y="tempo_total_desconectado_min",
                color="sensor",
                barmode="group",
                title="Tempo total desconectado por dia",
                template=PLOTLY_TEMPLATE,
            )
            grafico_diario_gap.update_layout(
                xaxis_title="dia", yaxis_title="min desconectado", legend_title=""
            )
            st.plotly_chart(grafico_diario_gap, use_container_width=True)

        st.dataframe(
            metricas_por_dia_filtradas.sort_values(["dia", "sensor"]),
            use_container_width=True,
            hide_index=True,
        )

    with tab_eventos:
        if eventos_filtrados.empty:
            st.info("Nao houve eventos de desconexao > 20 min no filtro atual.")
        else:
            timeline = px.timeline(
                eventos_filtrados.sort_values("inicio_desconexao"),
                x_start="inicio_desconexao",
                x_end="fim_desconexao",
                y="sensor",
                color="periodo_critico",
                hover_data=["duracao_min"],
                title="Linha do tempo das desconexoes",
                template=PLOTLY_TEMPLATE,
                color_discrete_map={True: "#d62728", False: "#1f77b4"},
            )
            timeline.update_layout(
                xaxis_title="periodo", yaxis_title="", legend_title="periodo critico"
            )
            st.plotly_chart(timeline, use_container_width=True)

            eventos_view = eventos_filtrados.copy()
            if "duracao_min" in eventos_view.columns:
                eventos_view["duracao_min"] = eventos_view["duracao_min"].round(2)
            st.dataframe(eventos_view, use_container_width=True, hide_index=True)

    with tab_series:
        if dados_normalizados_filtrados.empty:
            st.info("Nao ha serie normalizada para o filtro atual.")
        else:
            serie_chart = px.line(
                dados_normalizados_filtrados.sort_values("datahora"),
                x="datahora",
                y="valor",
                color="sensor",
                title="Serie normalizada por minuto",
                template=PLOTLY_TEMPLATE,
            )

            if mostrar_bruto and not dados_brutos_filtrados.empty:
                bruto_chart = px.scatter(
                    dados_brutos_filtrados.sort_values("datahora"),
                    x="datahora",
                    y="valor",
                    color="sensor",
                )
                for trace in bruto_chart.data:
                    trace.update(marker={"size": 3, "opacity": 0.35}, showlegend=False)
                    serie_chart.add_trace(trace)

            serie_chart.update_layout(
                xaxis_title="datahora",
                yaxis_title="valor",
                legend_title="",
            )
            st.plotly_chart(serie_chart, use_container_width=True)

    with tab_arquivos:
        st.markdown("**Inventario de CSVs carregados**")
        st.dataframe(inventory.sort_values(["sensor", "tipo"]), use_container_width=True, hide_index=True)

        if not resumo_geral.empty:
            st.markdown("**Resumo geral original**")
            st.dataframe(resumo_geral, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    render_dashboard()
