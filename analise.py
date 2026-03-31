import os
import time
import math
import requests
import pandas as pd

from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# =========================================================
# CONFIGURAÇÕES
# =========================================================

# Token hard-coded, como você pediu
UBIDOTS_TOKEN = "BBFF-fX2pCKEJ6Okgw6FZzD5JaJPPMXJHnU"

# Variáveis a analisar
VARIABLES = {
    "balcao_resfriado_1_calibrado": "689a49e13cb6f6190ce01d52",
    "balcao_resfriado_2": "6851c179075f22128df6d1ba",
    "balcao_congelado_calibrado": "689a4e7a3cb6f61e0c987781",
    "balcao_congelado": "6876b25c0fa5c0217ad091ea",
    "ambiente_loja": "67f8108a04592500111334c0",
}

# Cadastre aqui os períodos que você quer comparar
PERIODOS = {
    "depois": {
        "data_inicial": "2026-03-26",
        "data_final": "2026-03-29",
    },
    "antes": {
        "data_inicial": "2025-08-17",  # AJUSTE AQUI
        "data_final": "2025-08-26",    # AJUSTE AQUI
    },
}

# Troque só aqui
PERIODO_ATUAL = "antes"   # "antes" ou "depois"

TIMEZONE = "America/Sao_Paulo"

# Normalização local para 1 minuto
FREQUENCIA_NORMALIZADA = "1min"

# Regra de desconexão:
# 15 min + 5 min de tolerância = passou de 20 min
LIMIAR_DESCONEXAO_MIN = 20

# Faixa crítica
HORA_INICIO_CRITICA = 20
HORA_FIM_CRITICA = 8

# API Ubidots
BASE_URL = "https://industrial.api.ubidots.com/api/v1.6"
PAGE_SIZE = 1000
TIMEOUT = 60
MAX_RETRIES = 5

# Saída hard-coded
PASTA_SAIDA_BASE = Path(r"C:\relatorio_desconexoes\analise_desconexoes\loja\saida_ubidots_analise")

# =========================================================
# CONFIGURAÇÃO DERIVADA
# =========================================================

if PERIODO_ATUAL not in PERIODOS:
    raise ValueError(
        f"PERIODO_ATUAL='{PERIODO_ATUAL}' inválido. Use uma das chaves: {list(PERIODOS.keys())}"
    )

DATA_INICIAL = PERIODOS[PERIODO_ATUAL]["data_inicial"]
DATA_FINAL = PERIODOS[PERIODO_ATUAL]["data_final"]

PASTA_SAIDA = PASTA_SAIDA_BASE / PERIODO_ATUAL / f"{DATA_INICIAL}_a_{DATA_FINAL}"

# =========================================================
# FUNÇÕES DE APOIO
# =========================================================

def formatar_data_br(ts):
    if pd.isna(ts):
        return ""
    return ts.strftime("%d/%m/%Y %H:%M")


def slug(texto):
    return (
        str(texto)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace("-", "_")
    )


def esta_no_periodo_critico(ts):
    hora = ts.hour
    return hora >= HORA_INICIO_CRITICA or hora < HORA_FIM_CRITICA


def daterange(data_inicial, data_final):
    atual = data_inicial
    while atual <= data_final:
        yield atual
        atual += timedelta(days=1)


def parse_data_yyyy_mm_dd(data_str):
    return datetime.strptime(data_str, "%Y-%m-%d").date()


def inicio_fim_do_dia_em_ms(data_ref, tz_name):
    tz = ZoneInfo(tz_name)

    inicio = datetime(
        data_ref.year, data_ref.month, data_ref.day,
        0, 0, 0, tzinfo=tz
    )
    fim = datetime(
        data_ref.year, data_ref.month, data_ref.day,
        23, 59, 59, 999000, tzinfo=tz
    )

    inicio_ms = int(inicio.timestamp() * 1000)
    fim_ms = int(fim.timestamp() * 1000)

    return inicio_ms, fim_ms, inicio, fim


def request_json_com_retry(url, headers=None, params=None):
    ultima_exc = None

    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)

            if resp.status_code in (429, 500, 502, 503, 504):
                espera = min(2 ** tentativa, 30)
                print(
                    f"[WARN] HTTP {resp.status_code}. "
                    f"Tentativa {tentativa}/{MAX_RETRIES}. "
                    f"Aguardando {espera}s..."
                )
                time.sleep(espera)
                continue

            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            ultima_exc = e
            espera = min(2 ** tentativa, 30)
            print(
                f"[WARN] Falha na requisição. "
                f"Tentativa {tentativa}/{MAX_RETRIES}. "
                f"Aguardando {espera}s..."
            )
            time.sleep(espera)

    raise RuntimeError(
        f"Falha após {MAX_RETRIES} tentativas. Último erro: {ultima_exc}"
    )


def id_variavel_valido(variable_id: str) -> bool:
    if not variable_id:
        return False

    texto = str(variable_id).strip().lower()

    placeholders_invalidos = {
        "",
        "seu_id_da_variavel_aqui",
        "id_aqui",
        "coloque_aqui",
        "xxx",
        "none",
        "null",
    }

    return texto not in placeholders_invalidos


# =========================================================
# DOWNLOAD DOS DADOS BRUTOS DA UBIDOTS
# =========================================================

def buscar_dados_brutos_variavel_dia(variable_id, data_ref, tz_name):
    """
    Busca os dados brutos de UM DIA de UMA variável, com paginação.
    """
    inicio_ms, fim_ms, _, _ = inicio_fim_do_dia_em_ms(data_ref, tz_name)

    url = f"{BASE_URL}/variables/{variable_id}/values"
    headers = {
        "X-Auth-Token": UBIDOTS_TOKEN,
        "Content-Type": "application/json",
    }

    params_base = {
        "timestamp__gte": inicio_ms,
        "timestamp__lte": fim_ms,
        "page_size": PAGE_SIZE,
    }

    todos = []
    pagina = 1
    next_url = None

    while True:
        if next_url:
            payload = request_json_com_retry(next_url, headers=headers, params=None)
        else:
            params = dict(params_base)
            params["page"] = pagina
            payload = request_json_com_retry(url, headers=headers, params=params)

        if isinstance(payload, list):
            resultados = payload
            next_url = None
        else:
            resultados = payload.get("results", []) or payload.get("data", [])
            next_url = payload.get("next")

        if not resultados:
            break

        for item in resultados:
            ts = item.get("timestamp")
            valor = item.get("value")

            if ts is not None:
                todos.append({
                    "timestamp": ts,
                    "value": valor,
                })

        if next_url:
            time.sleep(0.15)
            continue

        if len(resultados) < PAGE_SIZE:
            break

        pagina += 1
        time.sleep(0.15)

    if not todos:
        return pd.DataFrame(columns=["timestamp", "value"])

    df = pd.DataFrame(todos)
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).copy()

    # Remove duplicados exatos de timestamp
    df = df.drop_duplicates(subset=["timestamp"]).copy()

    return df


def baixar_periodo_variavel(variable_id, nome_variavel, data_inicial_str, data_final_str, tz_name):
    data_inicial = parse_data_yyyy_mm_dd(data_inicial_str)
    data_final = parse_data_yyyy_mm_dd(data_final_str)

    frames = []

    print(f"\n=== Baixando variável: {nome_variavel} ({variable_id}) ===")

    for data_ref in daterange(data_inicial, data_final):
        print(f"Baixando dia {data_ref.strftime('%d/%m/%Y')}...")
        df_dia = buscar_dados_brutos_variavel_dia(variable_id, data_ref, tz_name)

        if not df_dia.empty:
            frames.append(df_dia)

    if not frames:
        return pd.DataFrame(columns=["timestamp", "value", "datahora"])

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["timestamp"]).copy()
    df["datahora"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(tz_name)
    df = df.sort_values("datahora").reset_index(drop=True)

    return df


# =========================================================
# NORMALIZAÇÃO PARA 1 MINUTO
# =========================================================

def normalizar_para_1_minuto(df_raw, data_inicial_str, data_final_str, tz_name):
    """
    Pega dados brutos (ex.: 10 em 10 segundos) e normaliza para 1 minuto via média.
    """
    tz = ZoneInfo(tz_name)

    data_inicial = parse_data_yyyy_mm_dd(data_inicial_str)
    data_final = parse_data_yyyy_mm_dd(data_final_str)

    inicio = datetime(
        data_inicial.year, data_inicial.month, data_inicial.day,
        0, 0, 0, tzinfo=tz
    )
    fim = datetime(
        data_final.year, data_final.month, data_final.day,
        23, 59, 0, tzinfo=tz
    )

    indice_completo = pd.date_range(start=inicio, end=fim, freq=FREQUENCIA_NORMALIZADA)

    if df_raw.empty:
        return pd.DataFrame({
            "datahora": indice_completo,
            "valor": [math.nan] * len(indice_completo)
        })

    df = df_raw.copy()
    df = df.set_index("datahora")[["value"]].sort_index()

    # Média por minuto
    df_1min = df.resample(FREQUENCIA_NORMALIZADA).mean()

    # Reindex para não perder minutos vazios
    df_1min = df_1min.reindex(indice_completo)
    df_1min = df_1min.rename(columns={"value": "valor"}).reset_index()
    df_1min = df_1min.rename(columns={"index": "datahora"})

    return df_1min


# =========================================================
# DETECÇÃO DE DESCONEXÕES
# =========================================================

def extrair_eventos_desconexao(df_1min):
    """
    Considera desconexão quando houver uma sequência de minutos sem dados
    com duração > LIMIAR_DESCONEXAO_MIN.
    """
    if df_1min.empty:
        return pd.DataFrame(columns=[
            "inicio_desconexao",
            "fim_desconexao",
            "duracao_min",
            "periodo_critico"
        ])

    s = df_1min.copy()
    s["sem_dado"] = s["valor"].isna()

    eventos = []

    em_falha = False
    inicio_falha = None
    contador = 0

    for _, row in s.iterrows():
        sem_dado = row["sem_dado"]
        ts = row["datahora"]

        if sem_dado and not em_falha:
            em_falha = True
            inicio_falha = ts
            contador = 1

        elif sem_dado and em_falha:
            contador += 1

        elif not sem_dado and em_falha:
            fim_falha = ts - pd.Timedelta(minutes=1)
            duracao_min = contador

            if duracao_min > LIMIAR_DESCONEXAO_MIN:
                periodo_critico = any(
                    esta_no_periodo_critico(minuto)
                    for minuto in pd.date_range(start=inicio_falha, end=fim_falha, freq="1min")
                )

                eventos.append({
                    "inicio_desconexao": inicio_falha,
                    "fim_desconexao": fim_falha,
                    "duracao_min": duracao_min,
                    "periodo_critico": periodo_critico,
                })

            em_falha = False
            inicio_falha = None
            contador = 0

    if em_falha:
        fim_falha = s.iloc[-1]["datahora"]
        duracao_min = contador

        if duracao_min > LIMIAR_DESCONEXAO_MIN:
            periodo_critico = any(
                esta_no_periodo_critico(minuto)
                for minuto in pd.date_range(start=inicio_falha, end=fim_falha, freq="1min")
            )

            eventos.append({
                "inicio_desconexao": inicio_falha,
                "fim_desconexao": fim_falha,
                "duracao_min": duracao_min,
                "periodo_critico": periodo_critico,
            })

    if not eventos:
        return pd.DataFrame(columns=[
            "inicio_desconexao",
            "fim_desconexao",
            "duracao_min",
            "periodo_critico"
        ])

    eventos_df = pd.DataFrame(eventos)
    eventos_df["inicio_desconexao_fmt"] = eventos_df["inicio_desconexao"].apply(formatar_data_br)
    eventos_df["fim_desconexao_fmt"] = eventos_df["fim_desconexao"].apply(formatar_data_br)

    return eventos_df


# =========================================================
# MÉTRICAS
# =========================================================

def calcular_metricas_periodo(df_1min, eventos_df, nome_variavel):
    total_minutos = len(df_1min)
    minutos_com_dado = int(df_1min["valor"].notna().sum())
    minutos_sem_dado = int(df_1min["valor"].isna().sum())

    pct_conectado = round((minutos_com_dado / total_minutos) * 100, 2) if total_minutos else 0
    pct_desconectado = round((minutos_sem_dado / total_minutos) * 100, 2) if total_minutos else 0

    qtd_desconexoes = len(eventos_df)

    if qtd_desconexoes > 0:
        menor_desconexao = round(float(eventos_df["duracao_min"].min()), 2)
        maior_desconexao = round(float(eventos_df["duracao_min"].max()), 2)
        media_desconexao = round(float(eventos_df["duracao_min"].mean()), 2)
        tempo_total_desconectado = round(float(eventos_df["duracao_min"].sum()), 2)
        qtd_criticas = int(eventos_df["periodo_critico"].sum())
    else:
        menor_desconexao = 0
        maior_desconexao = 0
        media_desconexao = 0
        tempo_total_desconectado = 0
        qtd_criticas = 0

    inicio_periodo = df_1min["datahora"].min() if not df_1min.empty else pd.NaT
    fim_periodo = df_1min["datahora"].max() if not df_1min.empty else pd.NaT

    return {
        "variavel": nome_variavel,
        "inicio_periodo": formatar_data_br(inicio_periodo),
        "fim_periodo": formatar_data_br(fim_periodo),
        "total_minutos_esperados": total_minutos,
        "minutos_com_dado": minutos_com_dado,
        "minutos_sem_dado": minutos_sem_dado,
        "percentual_conectado": pct_conectado,
        "percentual_desconectado": pct_desconectado,
        "quantidade_desconexoes_gt_20min": qtd_desconexoes,
        "menor_desconexao_min": menor_desconexao,
        "maior_desconexao_min": maior_desconexao,
        "media_desconexao_min": media_desconexao,
        "tempo_total_desconectado_min": tempo_total_desconectado,
        "desconexoes_no_periodo_critico_20h_08h": qtd_criticas,
    }


def calcular_metricas_por_dia(df_1min, eventos_df, nome_variavel):
    if df_1min.empty:
        return pd.DataFrame()

    df = df_1min.copy()
    df["dia"] = df["datahora"].dt.strftime("%d/%m/%Y")

    linhas = []

    for dia, grupo in df.groupby("dia", sort=True):
        inicio_dia = grupo["datahora"].min()
        fim_dia = grupo["datahora"].max()

        eventos_dia = eventos_df[
            (eventos_df["inicio_desconexao"] <= fim_dia) &
            (eventos_df["fim_desconexao"] >= inicio_dia)
        ].copy()

        total_minutos = len(grupo)
        minutos_com_dado = int(grupo["valor"].notna().sum())
        minutos_sem_dado = int(grupo["valor"].isna().sum())

        pct_conectado = round((minutos_com_dado / total_minutos) * 100, 2) if total_minutos else 0
        pct_desconectado = round((minutos_sem_dado / total_minutos) * 100, 2) if total_minutos else 0

        qtd_desconexoes = len(eventos_dia)

        if qtd_desconexoes > 0:
            menor_desconexao = round(float(eventos_dia["duracao_min"].min()), 2)
            maior_desconexao = round(float(eventos_dia["duracao_min"].max()), 2)
            media_desconexao = round(float(eventos_dia["duracao_min"].mean()), 2)
            tempo_total_desconectado = round(float(eventos_dia["duracao_min"].sum()), 2)
            qtd_criticas = int(eventos_dia["periodo_critico"].sum())
        else:
            menor_desconexao = 0
            maior_desconexao = 0
            media_desconexao = 0
            tempo_total_desconectado = 0
            qtd_criticas = 0

        linhas.append({
            "variavel": nome_variavel,
            "dia": dia,
            "total_minutos_esperados": total_minutos,
            "minutos_com_dado": minutos_com_dado,
            "minutos_sem_dado": minutos_sem_dado,
            "percentual_conectado": pct_conectado,
            "percentual_desconectado": pct_desconectado,
            "quantidade_desconexoes_gt_20min": qtd_desconexoes,
            "menor_desconexao_min": menor_desconexao,
            "maior_desconexao_min": maior_desconexao,
            "media_desconexao_min": media_desconexao,
            "tempo_total_desconectado_min": tempo_total_desconectado,
            "desconexoes_no_periodo_critico_20h_08h": qtd_criticas,
        })

    return pd.DataFrame(linhas)


# =========================================================
# SAÍDA
# =========================================================

def salvar_arquivos(nome_variavel, df_raw, df_1min, eventos_df, metricas_periodo, metricas_por_dia):
    base = Path(PASTA_SAIDA) / slug(nome_variavel)
    base.mkdir(parents=True, exist_ok=True)

    raw_out = df_raw.copy()
    if not raw_out.empty:
        raw_out["datahora_fmt"] = raw_out["datahora"].apply(formatar_data_br)
        raw_out = raw_out[["timestamp", "value", "datahora_fmt"]].rename(columns={
            "value": "valor",
            "datahora_fmt": "datahora"
        })
    raw_path = base / "dados_brutos.csv"
    raw_out.to_csv(raw_path, index=False, encoding="utf-8-sig")

    norm_out = df_1min.copy()
    if not norm_out.empty:
        norm_out["datahora"] = norm_out["datahora"].apply(formatar_data_br)
    norm_path = base / "dados_normalizados_1min.csv"
    norm_out.to_csv(norm_path, index=False, encoding="utf-8-sig")

    eventos_out = eventos_df.copy()
    if not eventos_out.empty:
        eventos_out = eventos_out[[
            "inicio_desconexao_fmt",
            "fim_desconexao_fmt",
            "duracao_min",
            "periodo_critico"
        ]].rename(columns={
            "inicio_desconexao_fmt": "inicio_desconexao",
            "fim_desconexao_fmt": "fim_desconexao",
            "duracao_min": "duracao_minutos",
            "periodo_critico": "periodo_critico_20h_08h"
        })
    eventos_path = base / "eventos_desconexao.csv"
    eventos_out.to_csv(eventos_path, index=False, encoding="utf-8-sig")

    resumo_df = pd.DataFrame([metricas_periodo])
    resumo_path = base / "metricas_periodo.csv"
    resumo_df.to_csv(resumo_path, index=False, encoding="utf-8-sig")

    por_dia_path = base / "metricas_por_dia.csv"
    metricas_por_dia.to_csv(por_dia_path, index=False, encoding="utf-8-sig")

    return {
        "raw": raw_path,
        "normalizado": norm_path,
        "eventos": eventos_path,
        "periodo": resumo_path,
        "por_dia": por_dia_path,
    }


# =========================================================
# EXECUÇÃO PRINCIPAL
# =========================================================

def main():
    if not UBIDOTS_TOKEN or UBIDOTS_TOKEN == "COLOQUE_SEU_TOKEN_AQUI":
        raise ValueError(
            "Defina seu token da Ubidots em UBIDOTS_TOKEN "
            "ou no próprio script."
        )

    print("\n" + "=" * 80)
    print("EXECUÇÃO DA ANÁLISE")
    print("=" * 80)
    print(f"Período atual: {PERIODO_ATUAL}")
    print(f"Data inicial : {DATA_INICIAL}")
    print(f"Data final   : {DATA_FINAL}")
    print(f"Pasta saída  : {PASTA_SAIDA}")
    print("=" * 80 + "\n")

    Path(PASTA_SAIDA).mkdir(parents=True, exist_ok=True)

    metricas_gerais = []

    for nome_variavel, variable_id in VARIABLES.items():
        if not id_variavel_valido(variable_id):
            print(f"[IGNORADO] Variável '{nome_variavel}' com ID inválido: {variable_id}")
            continue

        try:
            df_raw = baixar_periodo_variavel(
                variable_id=variable_id,
                nome_variavel=nome_variavel,
                data_inicial_str=DATA_INICIAL,
                data_final_str=DATA_FINAL,
                tz_name=TIMEZONE
            )

            df_1min = normalizar_para_1_minuto(
                df_raw=df_raw,
                data_inicial_str=DATA_INICIAL,
                data_final_str=DATA_FINAL,
                tz_name=TIMEZONE
            )

            eventos_df = extrair_eventos_desconexao(df_1min)

            metricas_periodo = calcular_metricas_periodo(
                df_1min=df_1min,
                eventos_df=eventos_df,
                nome_variavel=nome_variavel
            )

            metricas_por_dia = calcular_metricas_por_dia(
                df_1min=df_1min,
                eventos_df=eventos_df,
                nome_variavel=nome_variavel
            )

            caminhos = salvar_arquivos(
                nome_variavel=nome_variavel,
                df_raw=df_raw,
                df_1min=df_1min,
                eventos_df=eventos_df,
                metricas_periodo=metricas_periodo,
                metricas_por_dia=metricas_por_dia
            )

            metricas_gerais.append(metricas_periodo)

            print("\n" + "=" * 80)
            print(f"VARIÁVEL: {nome_variavel}")
            print("=" * 80)
            for k, v in metricas_periodo.items():
                print(f"{k}: {v}")

            print("\nArquivos gerados:")
            for nome, caminho in caminhos.items():
                print(f"- {nome}: {caminho}")

        except Exception as e:
            print(f"\n[ERRO] Falha ao processar '{nome_variavel}' ({variable_id}): {e}")
            continue

    if metricas_gerais:
        resumo_geral = pd.DataFrame(metricas_gerais)
        resumo_geral_path = Path(PASTA_SAIDA) / "resumo_geral.csv"
        resumo_geral.to_csv(resumo_geral_path, index=False, encoding="utf-8-sig")
        print(f"\nResumo geral salvo em: {resumo_geral_path}")
    else:
        print("\nNenhuma métrica foi gerada.")


if __name__ == "__main__":
    main()