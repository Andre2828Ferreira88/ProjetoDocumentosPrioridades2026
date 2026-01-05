import streamlit as st
import pandas as pd
import re
from pathlib import Path
import gspread
from google.oauth2.service_account import Credentials


# =========================================================
# CONFIG
# =========================================================
st.set_page_config(page_title="Validação de Prestadores", layout="wide")

# --- Visual minimalista Leroy ---
st.markdown(
    """
<style>
html, body, [class*="css"] {
    font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
    color: #1A1A1A;
}
.stApp { background-color: #F4F5F7; }
h1 { font-size: 30px; font-weight: 650; color:#1A1A1A; }
h2, h3 { color:#5C8F32; font-weight: 600; }
textarea, input {
    border-radius: 8px !important;
    border: 1px solid #E3E6EA !important;
    font-size: 14px !important;
}
.stButton>button{
    background:#7AC143; color:#fff; border-radius:8px; border:none;
    padding: 10px 18px; font-weight: 600;
}
.stButton>button:hover{ background:#5C8F32; }
[data-testid="stDataFrame"]{
    border-radius: 10px;
    border: 1px solid #E3E6EA;
    background: #FFFFFF;
}
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
""",
    unsafe_allow_html=True
)



# =========================================================
# FUNÇÕES UTILITÁRIAS
# =========================================================

def achar_coluna(df: pd.DataFrame, palavras):
    """Acha coluna por palavras-chave, ignorando quebras de linha e espaços."""
    palavras = [p.upper() for p in palavras]
    for col in df.columns:
        col_norm = str(col).upper().replace("\n", " ").strip()
        if all(p in col_norm for p in palavras):
            return col
    return None

def limpar_cnpj(valor) -> str:
    return re.sub(r"\D", "", str(valor or ""))

def formatar_cnpj(cnpj14: str) -> str:
    cnpj14 = limpar_cnpj(cnpj14)
    if len(cnpj14) != 14:
        return str(cnpj14)
    return f"{cnpj14[:2]}.{cnpj14[2:5]}.{cnpj14[5:8]}/{cnpj14[8:12]}-{cnpj14[12:]}"

def extrair_cnpjs_do_texto(texto: str):
    """Extrai CNPJs (com máscara e 14 dígitos puros) de qualquer texto."""
    if texto is None or (isinstance(texto, float) and pd.isna(texto)):
        return []

    s = str(texto)

    # captura CNPJ mascarado e também 14 dígitos puros
    mascarados = re.findall(r"\b\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}\b", s)
    puros = re.findall(r"\b\d{14}\b", s)

    encontrados = mascarados + puros
    out = []
    for c in encontrados:
        c14 = limpar_cnpj(c)
        if len(c14) == 14:
            out.append(c14)

    # de-dup mantendo ordem
    seen = set()
    final = []
    for x in out:
        if x not in seen:
            seen.add(x)
            final.append(x)
    return final

def extrair_cnpjs_da_busca(entrada: str):
    """Extrai CNPJs do texto digitado (aceita vários separadores)."""
    if not entrada:
        return []

    # quebra por quebra de linha, vírgula, ponto e vírgula e pipe
    pedaços = re.split(r"[\n,;|]+", entrada)

    cnpjs = []
    for p in pedaços:
        # se o usuário colar "CNPJ / CNPJ", ou texto junto, a gente extrai todos
        cnpjs.extend(extrair_cnpjs_do_texto(p))

    # de-dup mantendo ordem
    seen = set()
    final = []
    for x in cnpjs:
        if x not in seen:
            seen.add(x)
            final.append(x)
    return final

def safe_get(row, colname, default="-"):
    try:
        v = row.get(colname, default)
        if pd.isna(v):
            return default
        return v
    except Exception:
        return default
    
@st.cache_data(ttl=60)
def carregar_dados_google():
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

    creds = Credentials.from_service_account_file(
        "credentials.json",
        scopes=SCOPES
    )

    gc = gspread.authorize(creds)

    # ID da planilha (entre /d/ e /edit)
    SHEET_ID = "13a2-u3eW73NtRa5lYg_Kq5H37rYYjO1MIRcefuYSb4k"

    sh = gc.open_by_key(SHEET_ID)

    # Aba de respostas do Forms
    ws = sh.sheet1

    data = ws.get_all_records()
    df = pd.DataFrame(data)

    return df


# =========================================================
# CARREGAMENTO DE DADOS (CSV)
# =========================================================

DEFAULT_CSV = "Priorização de validação de documentos (respostas) - Respostas ao formulário 1.csv"

@st.cache_data(show_spinner=False)
def carregar_dados_csv(caminho_csv: str):
    # Corrige espaços acidentais no nome do arquivo
    caminho_csv = str(caminho_csv).strip()

    df = pd.read_csv(caminho_csv)

    # localizar coluna de CNPJ (pergunta do forms)
    col_cnpj_raw = achar_coluna(df, ["CNPJ", "PRIOR"])
    if not col_cnpj_raw:
        # fallback: tenta qualquer coluna com "CNPJ"
        col_cnpj_raw = achar_coluna(df, ["CNPJ"])
    if not col_cnpj_raw:
        raise KeyError("Não encontrei coluna de CNPJ no CSV.")

    # localizar coluna de timestamp do forms
    col_ts = achar_coluna(df, ["CARIMBO", "DATA"])
    if not col_ts:
        # fallback: tenta "DATA/HORA"
        col_ts = achar_coluna(df, ["DATA/HORA"])
    if not col_ts:
        # se não achar, cria uma coluna "ordem" para não quebrar
        df["_TS_ORD"] = range(len(df))
        col_ts = "_TS_ORD"
    else:
        # parse para datetime (robusto)
        df[col_ts] = pd.to_datetime(df[col_ts], errors="coerce", dayfirst=True)
    
def carregar_dados_from_df(df):
    # localizar coluna de CNPJ
    col_cnpj_raw = achar_coluna(df, ["CNPJ", "PRIOR"]) or achar_coluna(df, ["CNPJ"])
    if not col_cnpj_raw:
        raise KeyError("Não encontrei coluna de CNPJ.")

    # localizar coluna de timestamp
    col_ts = achar_coluna(df, ["CARIMBO", "DATA"]) or achar_coluna(df, ["DATA/HORA"])
    if not col_ts:
        df["_TS_ORD"] = range(len(df))
        col_ts = "_TS_ORD"
    else:
        df[col_ts] = pd.to_datetime(df[col_ts], errors="coerce", dayfirst=True)

    # extrair CNPJs
    df["_CNPJS_LISTA"] = df[col_cnpj_raw].apply(extrair_cnpjs_do_texto)

    df_expl = df.explode("_CNPJS_LISTA").rename(columns={"_CNPJS_LISTA": "CNPJ_14"})
    df_expl["CNPJ_14"] = df_expl["CNPJ_14"].astype(str).apply(limpar_cnpj)
    df_expl = df_expl[df_expl["CNPJ_14"].str.len() == 14].copy()

    df_expl = df_expl.sort_values(col_ts, ascending=False)\
                     .drop_duplicates("CNPJ_14", keep="first")

    return df_expl, col_ts


    # extrair CNPJs (pode ter texto misturado)
    df["_CNPJS_LISTA"] = df[col_cnpj_raw].apply(extrair_cnpjs_do_texto)

    # Explode para ficar 1 linha por CNPJ (facilita busca e performance)
    df_expl = df.explode("_CNPJS_LISTA").rename(columns={"_CNPJS_LISTA": "CNPJ_14"})
    df_expl["CNPJ_14"] = df_expl["CNPJ_14"].astype(str).apply(limpar_cnpj)
    df_expl = df_expl[df_expl["CNPJ_14"].str.len() == 14].copy()

    # Mantém só o registro mais recente por CNPJ
    df_expl = df_expl.sort_values(col_ts, ascending=False).drop_duplicates("CNPJ_14", keep="first")

    return df_expl, col_ts


# =========================================================
# APP
# =========================================================
st.title("Validação de Prestadores")
st.caption("Consulta rápida do status de validação de documentos — uso interno Leroy Merlin")

# Carrega dados
try:
    df_raw = carregar_dados_google()
    df_base, COL_TS = carregar_dados_from_df(df_raw)
except Exception as e:
    st.error(f"❌ Erro ao carregar CSV: {e}")
    st.stop()

# Campo de busca
entrada = st.text_area(
    "CNPJ do prestador",
    placeholder="Cole 1 ou vários CNPJs (com ou sem máscara). Ex: 00.000.000/0000-00 / 11.111.111/1111-11",
    height=90
)

# Botão (opcional, mas deixa mais profissional do que auto-run)
colA, colB, colC = st.columns([1, 1, 6])
with colA:
    buscar = st.button("Consultar")

def status_final(row):
    status_prest = str(row.get("STATUS PRESTADOR", "")).upper()
    status_inst = str(row.get("STATUS INSTALADOR", "")).upper()
    status_comp = str(row.get("STATUS COMPLETO", "")).upper()
    obs = str(row.get("OBSERVAÇÃO", "")).strip()
    prob = str(row.get("PROBLEMAS", "")).upper()

    # Não encontrado
    if status_comp == "NÃO ENCONTRADO":
        return "🔴 NÃO ENCONTRADO"

    # Problema grave (CPF / observação)
    if "CPF" in prob or obs not in ("", "-"):
        return "🔴 DOCUMENTOS COM PENDÊNCIA"

    # Não analisado ainda
    if status_prest in ("-", "ND", "") and status_inst in ("-", "ND", ""):
        return "🟡 AGUARDANDO ANÁLISE"

    # Pendente
    if "PENDENTE" in status_prest or "PENDENTE" in status_inst:
        return "🟡 DOCUMENTOS PENDENTES"

    # Aprovado
    if "ACEITO" in status_prest and "ACEITO" in status_inst:
        return "🟢 DOCUMENTOS PÓS-ANÁLISE"

    return "⚠️ INCONSISTENTE"




def cor_status(val):
    v = str(val).upper()
    if "ACEITO" in v or "APROVADO" in v:
        return "background-color: #c6efce"
    if "PENDENTE" in v:
        return "background-color: #ffeb9c"
    if "NÃO" in v or "REPROV" in v or "NEGAD" in v:
        return "background-color: #f4cccc"
    return ""

if buscar or (entrada and entrada.strip()):
    cnpjs_busca = extrair_cnpjs_da_busca(entrada)

    if not cnpjs_busca:
        st.warning("Nenhum CNPJ válido encontrado na busca.")
        st.stop()

    resultados = []

    # Descobrir colunas (tolerante a pequenas mudanças)
    col_status_completo = achar_coluna(df_base, ["STATUS", "COMPLETO"]) or "STATUS COMPLETO"
    col_status_prestador = achar_coluna(df_base, ["STATUS", "ACEITO/PENDENTE"]) or "STATUS (ACEITO/PENDENTE)"
    col_status_instalador = achar_coluna(df_base, ["STATUS", "ACEITO/PENDENTE)2"]) or "STATUS (ACEITO/PENDENTE)2"
    col_resp = achar_coluna(df_base, ["RESPONSAVEL"]) or achar_coluna(df_base, ["RESPONSAVEL", "VERIFIC"]) or "responsavel pela verificação"
    col_data_verif = achar_coluna(df_base, ["DATA", "VERIFIC"]) or "data de verificação dos documentos"
    col_prob_desc = achar_coluna(df_base, ["PROBLEMAS", "DESCRI"])
    col_prob_cpf = achar_coluna(df_base, ["PROBLEMA", "CPF"])
    col_obs = (
    achar_coluna(df_base, ["OBSERVA"]) or
    achar_coluna(df_base, ["OBSERVAÇÃO"]) or
    achar_coluna(df_base, ["OBSERVACAO"]))


    for cnpj14 in cnpjs_busca:
        linha = df_base[df_base["CNPJ_14"] == cnpj14]

        if linha.empty:
            resultados.append({
                "CNPJ": formatar_cnpj(cnpj14),
                "STATUS COMPLETO": "NÃO ENCONTRADO",
                "STATUS PRESTADOR": "-",
                "STATUS INSTALADOR": "-",
                "RESPONSÁVEL": "-",
                "DATA VERIFICAÇÃO": "-",
                "PROBLEMAS": "-",
                "ATUALIZADO EM": "-"
            })
        else:
            r = linha.iloc[0]

            status_completo = safe_get(r, col_status_completo, "-")
            status_prest = safe_get(r, col_status_prestador, "-")
            status_inst = safe_get(r, col_status_instalador, "-")
            responsavel = safe_get(r, col_resp, "-")
            data_verif = safe_get(r, col_data_verif, "-")
            prob_desc = safe_get(r, col_prob_desc, "-") if col_prob_desc else "-"
            prob_cpf = safe_get(r, col_prob_cpf, "-") if col_prob_cpf else "-"

            problemas = []

            if str(prob_cpf).upper() == "SIM":
                problemas.append("Problema no CPF")

            if prob_desc and prob_desc != "-":
                problemas.append(str(prob_desc))

            problemas = " | ".join(problemas) if problemas else "-"

            raw_obs = safe_get(r, col_obs, "")
            observacao = raw_obs.strip() if isinstance(raw_obs, str) and raw_obs.strip() else "-"



            atualizado_em = r[COL_TS]
            if pd.isna(atualizado_em):
                atualizado_em = "-"
            else:
                atualizado_em = pd.to_datetime(atualizado_em).strftime("%d/%m/%Y %H:%M")

            resultados.append({
                "CNPJ": formatar_cnpj(cnpj14),
                "STATUS COMPLETO": status_completo if str(status_completo).strip() else "-",
                "STATUS PRESTADOR": status_prest if str(status_prest).strip() else "-",
                "STATUS INSTALADOR": status_inst if str(status_inst).strip() else "-",
                "OBSERVAÇÃO": observacao if str(observacao).strip() else "-",
                "RESPONSÁVEL": responsavel if str(responsavel).strip() else "-",
                "DATA VERIFICAÇÃO": data_verif if str(data_verif).strip() else "-",
                "PROBLEMAS": problemas if str(problemas).strip() else "-",
                "ATUALIZADO EM": atualizado_em
            })


    resultado_df = pd.DataFrame(resultados)

    # status final
    resultado_df["STATUS FINAL"] = resultado_df.apply(status_final, axis=1)

    # Ordena colunas
    resultado_df = resultado_df[
        ["CNPJ", "STATUS FINAL", "STATUS COMPLETO", "STATUS PRESTADOR",
        "STATUS INSTALADOR", "OBSERVAÇÃO",
        "RESPONSÁVEL", "DATA VERIFICAÇÃO",
        "ATUALIZADO EM", "PROBLEMAS"]
    ]


    st.subheader("Resultado da Consulta")
    st.dataframe(
        resultado_df.style.applymap(
            cor_status,
            subset=["STATUS COMPLETO", "STATUS PRESTADOR", "STATUS INSTALADOR", "STATUS FINAL"]
        ),
        use_container_width=True
    )

    # KPI simples no topo (opcional, mas ajuda diretoria)
    aprov = (resultado_df["STATUS FINAL"].str.contains("APROVADO")).sum()
    pend = (resultado_df["STATUS FINAL"].str.contains("PENDENTE")).sum()
    nao = (resultado_df["STATUS FINAL"].str.contains("NÃO ENCONTRADO")).sum()

    c1, c2, c3 = st.columns(3)
    c1.metric("Aprovados", int(aprov))
    c2.metric("Pendentes", int(pend))
    c3.metric("Não encontrados", int(nao))
    