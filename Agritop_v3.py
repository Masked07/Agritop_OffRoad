# dashboard_agritop_prioritarios_final.py
# python -m streamlit run "Y:\GERENCIA\BR_DOLS_OPER_ATEX\NP-1\29 - Atendimento Logístico\L3\Agritop_v3.py"
"""
Dashboard offline — Clientes Prioritários (Vibra Agritop / Vibra Diesel)
Versão final integrada: mantém sua lógica, adiciona visão gerencial (Top5, OTIF,
contagem por combustível, pizza de materiais, filtros Base/N2) e estilização.

Colunas esperadas (conforme confirmação do usuário):
- Código do emissor  -> normalized: codigo_do_emissor
- Cliente_Nome       -> normalized: cliente_nome
- N2                 -> normalized: n2
- Material           -> normalized: material
- Ordem de venda     -> normalized: ordem_de_venda (detectado automaticamente)

Observações:
- O script normaliza cabeçalhos para snake_case no início (função normalize_df_cols),
  então todas as referências internas usam nomes normalizados.
- Cores do tema definidas conforme solicitação do usuário.
"""
from datetime import datetime
from pathlib import Path
import zipfile
import io
import typing as t

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px

# ---------------------------------------------------------
# Caminhos relativos para rodar no GitHub + Streamlit Cloud
# ---------------------------------------------------------
DATA_DIR = Path("data")

DEFAULT_MAIN = DATA_DIR / "Extrato Analitico.xlsx"
DEFAULT_BLOQ = DATA_DIR / "Pedidos Bloqueados.xlsx"
DEFAULT_OTIF = DATA_DIR / "OTIF.xlsx"

PRIORITY_MATERIALS = ["VIBRA  AGRITOP", "Vibra Diesel Off-Road"]

# Tema de cores (do usuário)
COLORS = {
    "verde_escuro": "#044317",
    "verde_claro": "#268200",
    "amarelo": "#FEDC00",
    "azul": "#0000FF",
}

# ----------------------------
# Utilitários
# ----------------------------
st.set_page_config(layout="wide", page_title="Dashboard Prioritários — Agritop/Vibra")


def normalize_colname(c: str) -> str:
    c = str(c or "")
    replacements = {
        "ç": "c", "ã": "a", "á": "a", "à": "a", "â": "a", "é": "e",
        "ê": "e", "í": "i", "ó": "o", "õ": "o", "ô": "o", "ú": "u",
        "Á": "a", "É": "e", "Í": "i", "Ó": "o", "Ú": "u"
    }
    for k, v in replacements.items():
        c = c.replace(k, v).replace(k.upper(), v)
    return (
        c.strip()
         .lower()
         .replace(" ", "_")
         .replace(".", "")
         .replace("/", "_")
         .replace("-", "_")
         .replace("(", "")
         .replace(")", "")
    )


def normalize_df_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return None
    df = df.copy()
    df.columns = [normalize_colname(c) for c in df.columns]
    return df


def safe_read(path: Path) -> t.Optional[pd.DataFrame]:
    try:
        if not path.exists():
            return None
        if path.suffix.lower() in [".xls", ".xlsx"]:
            return pd.read_excel(path)
        elif path.suffix.lower() == ".csv":
            return pd.read_csv(path, dtype=str)
        else:
            return None
    except Exception as e:
        st.error(f"Erro lendo {path}: {e}")
        return None


def clean_ov_series(s: pd.Series) -> pd.Series:
    return (
        s.fillna("").astype(str)
         .str.strip()
         .str.replace('\xa0', '', regex=False)
         .str.replace(' ', '', regex=False)
         .str.lstrip('0')
    )


def to_datetime_cols(df: pd.DataFrame, cols: t.List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", dayfirst=True)
    return df


# Classificação centralizada (mantém lógica do seu classify_status)
def classify_status_normalized(row, today: datetime):
    motivo_recusa = str(row.get("motivo_de_recusa", "")).strip()
    bloqueio = str(row.get("bloqueio", "")).strip().lower()
    tipo_bloq = str(row.get("tipo_de_bloqueio", "")).strip()

    data_liberacao = row.get("dt_liberacao") or row.get("data_liberação") or row.get("data_liberacao")
    data_faturamento = row.get("data_do_faturamento")
    num_transporte = row.get("numero_do_transporte") or row.get("numero_remessa")

    data_prevista = row.get("data_prevista_entrega")
    data_remessa = row.get("data_desejada_da_remessa")

    # 1) BLOQUEADO
    if (
        tipo_bloq not in ["", "nan", "NaN", None]
        or (bloqueio == "sim" and pd.isna(data_liberacao))
    ):
        return "Bloqueado"

    # 2) CANCELADO
    if motivo_recusa not in ["", "0", "nan", "NaN", None]:
        return "Cancelado"

    # 3) FATURADO
    if pd.notnull(data_faturamento):
        return "Faturado"

    # 4) PROGRAMADO
    if pd.notnull(num_transporte):
        return "Programado"

    # 5) DATA PREVISTA
    if pd.notnull(data_prevista):
        try:
            return (
                "Entrega para data futura"
                if data_prevista.date() > today.date()
                else "Verificar retorno de frota"
            )
        except Exception:
            pass

    # 6) DATA REMESSA
    if pd.notnull(data_remessa):
        try:
            return (
                "Data futura"
                if data_remessa.date() > today.date()
                else "Verificar retorno de frota"
            )
        except Exception:
            return "Verificar retorno de frota"

    # 7) INDEFINIDO
    return "Indefinido"


# Merge helpers

def smart_merge_main_and_bloq(df_main: pd.DataFrame, df_bloq: pd.DataFrame) -> pd.DataFrame:
    if df_bloq is None:
        return df_main
    if 'ordem_de_venda' in df_main.columns and 'ordem_de_venda' in df_bloq.columns:
        keep = [c for c in ['ordem_de_venda','tipo_de_bloqueio','bloq_financ','bloq_comercial','adequacao'] if c in df_bloq.columns]
        if keep:
            return df_main.merge(df_bloq[keep].drop_duplicates(), on='ordem_de_venda', how='left')
    return df_main


def merge_otif(df_main: pd.DataFrame, df_otif: pd.DataFrame) -> pd.DataFrame:
    # keep minimal columns from otif if present
    if df_otif is None:
        return df_main
    # detect ofensor-like col
    ofensor_col = next((c for c in df_otif.columns if 'ofensor' in c.lower()), None)
    pick = ['ordem_de_venda']
    if ofensor_col:
        pick.append(ofensor_col)
    # some otif files may have a calendar datetime col
    date_col = next((c for c in df_otif.columns if 'data_hora_criacao_da_ov_calendario' in c.lower()), None)
    if date_col:
        pick.append(date_col)
    pick = [c for c in pick if c in df_otif.columns]
    if 'ordem_de_venda' in df_main.columns and pick:
        return df_main.merge(df_otif[pick].drop_duplicates(), on='ordem_de_venda', how='left')
    return df_main


# Export helpers

def export_by_sapcode(df: pd.DataFrame, sap_col: str, out_root: str = "exports") -> t.List[str]:
    date_str = datetime.now().strftime("%Y-%m-%d")
    root = Path(out_root) / date_str
    root.mkdir(parents=True, exist_ok=True)
    written = []
    for code, grp in df.groupby(sap_col):
        safe_code = str(code).strip() or "SEM_COD"
        filename = root / f"{safe_code}.xlsx"
        try:
            grp.to_excel(filename, index=False)
            written.append(str(filename))
        except Exception as e:
            st.error(f"Erro salvando {filename}: {e}")
    return written


def make_zip(paths_list: t.List[str]) -> io.BytesIO:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w") as z:
        for p in paths_list:
            z.write(p, arcname=Path(p).name)
    buf.seek(0)
    return buf


# ----------------------------
# UI / Carregamento de arquivos
# ----------------------------
st.title("Entrega de Produtos Claros - Clientes Agritop / Off Road")
st.markdown("B2B - Mid/Light")

st.sidebar.header("Fonte de dados")
load_mode = st.sidebar.radio("Modo", ["Usar caminhos padrão (Y:...)", "Upload manual", "Pasta local"])

# carregar arquivos
df_main = df_bloq = df_otif = None

if load_mode == "Usar caminhos padrão (Y:...)":
    if DEFAULT_MAIN.exists():
        df_main = safe_read(DEFAULT_MAIN)
        st.sidebar.write(f"Carregado: {DEFAULT_MAIN.name}")
    else:
        st.sidebar.warning(f"Arquivo principal não encontrado: {DEFAULT_MAIN}")
    if DEFAULT_BLOQ.exists():
        df_bloq = safe_read(DEFAULT_BLOQ)
        st.sidebar.write(f"Carregado: {DEFAULT_BLOQ.name}")
    if DEFAULT_OTIF.exists():
        df_otif = safe_read(DEFAULT_OTIF)
        st.sidebar.write(f"Carregado: {DEFAULT_OTIF.name}")

elif load_mode == "Upload manual":
    f_main = st.sidebar.file_uploader("Extrato Analítico (xlsx/csv)", type=["xlsx","csv"])
    f_bloq = st.sidebar.file_uploader("Pedidos Bloqueados (opcional)", type=["xlsx","csv"])
    f_otif = st.sidebar.file_uploader("OTIF (opcional)", type=["xlsx","csv"])
    if f_main:
        try:
            df_main = pd.read_excel(f_main) if f_main.name.lower().endswith(("xls","xlsx")) else pd.read_csv(f_main, dtype=str)
        except Exception as e:
            st.error(f"Erro lendo arquivo principal: {e}")
    if f_bloq:
        try:
            df_bloq = pd.read_excel(f_bloq) if f_bloq.name.lower().endswith(("xls","xlsx")) else pd.read_csv(f_bloq, dtype=str)
        except Exception as e:
            st.error(f"Erro lendo bloq: {e}")
    if f_otif:
        try:
            df_otif = pd.read_excel(f_otif) if f_otif.name.lower().endswith(("xls","xlsx")) else pd.read_csv(f_otif, dtype=str)
        except Exception as e:
            st.error(f"Erro lendo otif: {e}")

else:  # pasta local
    folder = st.sidebar.text_input("Caminho da pasta local (ex: Y:/...)", value=str(DEFAULT_MAIN.parent))
    if folder:
        p = Path(folder)
        if p.exists() and p.is_dir():
            for file in p.iterdir():
                name = file.name.lower()
                try:
                    if "extrato" in name or "analit" in name:
                        df_main = safe_read(file)
                    if "bloque" in name:
                        df_bloq = safe_read(file)
                    if "otif" in name:
                        df_otif = safe_read(file)
                except Exception:
                    continue
        else:
            st.error("Pasta inválida.")

# obrigatórios
if df_main is None:
    st.error("Arquivo principal (Extrato Analítico) não carregado. Forneça o arquivo via Upload, Pasta local ou coloque no caminho padrão.")
    st.stop()

# Normalize columns — usar nomes normalizados internamente
df_main = normalize_df_cols(df_main)
if df_bloq is not None:
    df_bloq = normalize_df_cols(df_bloq)
if df_otif is not None:
    df_otif = normalize_df_cols(df_otif)

# Detect and clean 'ordem_de_venda'
possible_ordem = [c for c in df_main.columns if "ordem" in c.lower() and "venda" in c.lower()]
if not possible_ordem:
    st.error("Não foi encontrada a coluna de Ordem de Venda no Extrato Analítico (nomes esperados contendo 'ordem' e 'venda'). Ajuste o cabeçalho.")
    st.stop()
ordem_col = possible_ordem[0]
df_main['ordem_de_venda'] = clean_ov_series(df_main[ordem_col])

if df_bloq is not None:
    possible_ordem_b = [c for c in df_bloq.columns if "ordem" in c.lower() and "venda" in c.lower()]
    if possible_ordem_b:
        df_bloq['ordem_de_venda'] = clean_ov_series(df_bloq[possible_ordem_b[0]])

if df_otif is not None:
    possible_ordem_o = [c for c in df_otif.columns if "ordem" in c.lower() and "venda" in c.lower()]
    if possible_ordem_o:
        df_otif['ordem_de_venda'] = clean_ov_series(df_otif[possible_ordem_o[0]])

# Convert date-like columns
common_dates = [
    'data_prevista_entrega', 'data_do_faturamento', 'dt_liberacao',
    'data_desejada_da_remessa', 'data_hora_criacao_da_ov_calendario'
]

df_main = to_datetime_cols(df_main, [c for c in common_dates if c in df_main.columns])
if df_otif is not None:
    df_otif = to_datetime_cols(df_otif, [c for c in common_dates if c in df_otif.columns])

# Detect material column
material_candidates = [c for c in df_main.columns if 'material' in c.lower()]
if not material_candidates:
    st.error("Coluna 'Material' não encontrada no Extrato Analítico (procure por cabeçalhos contendo 'material').")
    st.stop()
material_col = material_candidates[0]

# preparar material maiúsculo para comparações
df_main[material_col] = (
    df_main[material_col]
    .astype(str)
    .str.strip()
    .str.upper()
)

# marcar materiais prioritários
df_main['is_priority_material'] = df_main[material_col].isin([m.upper() for m in PRIORITY_MATERIALS])

# ===========================
# SEGMENTAÇÃO POR ANO E MÊS
# ===========================

# Detectar coluna de data de remessa
remessa_candidates = [c for c in df_main.columns if 'remessa' in c.lower()]
data_remessa_col = remessa_candidates[0] if remessa_candidates else None

if data_remessa_col:
    # Garantir tipo datetime
    df_main[data_remessa_col] = pd.to_datetime(df_main[data_remessa_col], errors='coerce')

    # ===== FILTRO POR ANO =====
    anos_disponiveis = (
        df_main[data_remessa_col]
        .dt.year
        .dropna()
        .unique()
        .tolist()
    )
    anos_disponiveis = sorted([int(a) for a in anos_disponiveis])

    ano_selecionado = st.selectbox(
        "Ano",
        options=anos_disponiveis,
        index=len(anos_disponiveis) - 1
    )

    df_main = df_main[df_main[data_remessa_col].dt.year == ano_selecionado]

    # ===== FILTRO POR MÊS =====
    meses_disponiveis = (
        df_main[data_remessa_col]
        .dt.month
        .dropna()
        .unique()
        .tolist()
    )
    meses_disponiveis = sorted([int(m) for m in meses_disponiveis])

    # Dicionário para nome dos meses
    nome_meses = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }

    meses_legiveis = [f"{m:02d} - {nome_meses[m]}" for m in meses_disponiveis]

    seletor_mes = st.selectbox(
        "Mês",
        options=meses_legiveis,
        index=len(meses_legiveis) - 1
    )

    mes_selecionado = int(seletor_mes.split(" - ")[0])

    df_main = df_main[df_main[data_remessa_col].dt.month == mes_selecionado]

else:
    st.warning("Coluna 'data de remessa' não encontrada para os filtros de ano/mês.")

# === Cliente: usar somente coluna codigo_do_emissor (detectada automaticamente) ===
client_candidates = [c for c in df_main.columns if "codigo_do_emissor" in c.lower() or "codigo_do_emissor" == c]
if not client_candidates:
    # try other likely names
    client_candidates = [c for c in df_main.columns if "codigo" in c.lower() and "recebedor" in c.lower()]
if not client_candidates:
    st.error("A coluna 'codigo_do_emissor' não foi encontrada na base. Verifique o cabeçalho.")
    st.stop()
client_col = client_candidates[0]

# detect other useful columns (razao_social / cliente_nome)
razao_candidates = [c for c in df_main.columns if 'cliente_nome' in c or 'razao' in c or 'cliente' in c and 'nome' in c]
razao_col = razao_candidates[0] if razao_candidates else None

# detect diretoria n2
n2_candidates = [c for c in df_main.columns if c.lower() == 'n2' or 'n2' in c.lower()]
n2_col = n2_candidates[0] if n2_candidates else None

# marcar clientes prioritários (todos que tiveram ao menos 1 pedido de material prioritário)
clients_with_priority = (
    df_main.loc[df_main['is_priority_material'], client_col]
    .dropna()
    .unique()
    .tolist()
)

df_main['cliente_prioritario'] = df_main[client_col].isin(clients_with_priority)

# filtrar apenas pedidos desses clientes prioritários
df_prior = df_main[df_main['cliente_prioritario'] == True].copy()
if df_prior.empty:
    st.warning("Nenhum cliente prioritário identificado.")
    st.stop()

# Merge com bloqueios e com OTIF (adiciona colunas se existirem)
df_prior = smart_merge_main_and_bloq(df_prior, df_bloq)
df_prior = merge_otif(df_prior, df_otif)

# Classificar status (mantendo lógica)
today = datetime.today()
df_prior['status_check'] = df_prior.apply(lambda r: classify_status_normalized(r, today), axis=1)

# detectar coluna SAP (codigo emissor) para ordenação
sap_candidates = [c for c in df_prior.columns if "codigo_do_emissor" in c.lower()]
sap_col_default = sap_candidates[0] if sap_candidates else None
sap_col = st.sidebar.selectbox(
    "Coluna Código SAP (para ordenação)",
    options=([sap_col_default] + sap_candidates) if sap_col_default else (sap_candidates or ['ordem_de_venda']),
    index=0
)
if sap_col not in df_prior.columns:
    sap_col = 'ordem_de_venda'

# ordenar
df_prior.sort_values(by=[sap_col, 'ordem_de_venda'], inplace=True, na_position='last')

# ----------------------------
# VISÃO GERENCIAL INTEGRADA – LAYOUT AJUSTADO
# ----------------------------

aba1, aba2 = st.tabs(["📊 Visão Gerencial", "⚙️ Visão Operacional"])

with aba1:
    st.header("Visão Gerencial — Produtos Claros")

    df_viz = df_prior.copy()

    # detectar colunas usadas nos filtros
    remessa_candidates = [c for c in df_viz.columns if 'remessa' in c.lower()]
    data_remessa_col = remessa_candidates[0] if remessa_candidates else None
    dir_col = n2_col  
    seg_candidates = [c for c in df_viz.columns if 'segment' in c.lower() or 'segmento' in c.lower()]
    segmento_col = seg_candidates[0] if seg_candidates else None

    # --------------------------------------------------------
    # 🔽 FILTROS — EM 3 COLUNAS
    # --------------------------------------------------------
    st.subheader("Filtros")

    col_f1, col_f2= st.columns(2)

    selected_year = None
    selected_month = None

    with col_f1:
        # Ano
        if data_remessa_col:
            years = sorted(df_viz[data_remessa_col].dt.year.dropna().unique().tolist())
            selected_year = st.selectbox("Ano", years, index=len(years)-1)

    with col_f2:
        # Mês
        if data_remessa_col and selected_year:
            df_year = df_viz[df_viz[data_remessa_col].dt.year == selected_year]
            months = sorted(df_year[data_remessa_col].dt.month.dropna().unique().tolist())
            nome_meses = {1:"Jan",2:"Fev",3:"Mar",4:"Abr",5:"Mai",6:"Jun",7:"Jul",8:"Ago",9:"Set",10:"Out",11:"Nov",12:"Dez"}
            months_lbl = [f"{m:02d} - {nome_meses[m]}" for m in months]

            if months_lbl:
                sel = st.selectbox("Mês", months_lbl, index=len(months_lbl)-1)
                selected_month = int(sel.split(" - ")[0])

    # Aplicar filtros
    if data_remessa_col and selected_year:
        df_viz = df_viz[df_viz[data_remessa_col].dt.year == selected_year]
    if data_remessa_col and selected_month:
        df_viz = df_viz[df_viz[data_remessa_col].dt.month == selected_month]
    
    # -----------------------
    # RIGHT: KPIs (4 big numbers) + TOP10 TABELA
    # -----------------------
        st.subheader("Pedidos")

        # VISUAL - NOVO: preparar contagens para materiais prioritários e demais
        # criar df_priority e df_nonpriority (apenas cópias locais)
        df_priority = df_viz[df_viz['is_priority_material'] == True].copy()
        df_nonpriority = df_viz[df_viz['is_priority_material'] == False].copy()

        # Total de pedidos do material prioritário (linhas)
        total_pedidos_priority = int(df_priority.shape[0])

        # OTIF para prioritários (calcular por OV se possível, similar à lógica existente)
        # Agrupar por ordem_de_venda e considerar 'otif_atendido' se existir (defensivo)
        def compute_ov_otif(df_source):
            if 'ordem_de_venda' not in df_source.columns:
                return 0, 0.0
            if 'otif_atendido' in df_source.columns:
                agg = df_source.groupby('ordem_de_venda', as_index=False)['otif_atendido'].min()
                tot_ov = agg['ordem_de_venda'].nunique()
                tot_otif = int(agg['otif_atendido'].sum())
                perc = (tot_otif / tot_ov) * 100 if tot_ov > 0 else 0.0
                return tot_ov, perc
            else:
                # se não existir col. otif, tentar usar df_otif por merge (defensivo)
                if df_otif is not None and 'ordem_de_venda' in df_otif.columns:
                    tmp = df_source[['ordem_de_venda']].drop_duplicates().merge(
                        df_otif[['ordem_de_venda']].drop_duplicates(), on='ordem_de_venda', how='left', indicator=True
                    )
                    # não temos ofensor, então marcar 0%
                    tot_ov = tmp['ordem_de_venda'].nunique()
                    return tot_ov, 0.0
                return df_source['ordem_de_venda'].nunique(), 0.0

        tot_ov_prio, perc_otif_prio = compute_ov_otif(df_priority)
        tot_ov_nonprio, perc_otif_nonprio = compute_ov_otif(df_nonpriority)

        # Total pedidos demais materiais (linhas)
        total_pedidos_nonpriority = int(df_nonpriority.shape[0])

        # Display KPIs em 4 colunas
        b1, b2, b3, b4 = st.columns(4)
        b1.metric("Pedidos - Agritop", f"{total_pedidos_priority:,}")
        b2.metric("OTIF - Agritop (%)", f"{perc_otif_prio:,.2f}%")
        b3.metric("Pedidos - Demais Produtos", f"{total_pedidos_nonpriority:,}")
        b4.metric("OTIF - Demais Produtos (%)", f"{perc_otif_nonprio:,.2f}%")

        st.markdown("---")

        # -----------------------
        # TOP 10 clientes prioritários (ordenado por quantidade de pedidos do material prioritário)
        # -----------------------
        st.subheader("TOP 10 Clientes Agritop")

        # Preparar agregações necessárias para a tabela (defensivo quanto a nomes de colunas)
        cliente_name_col = razao_col if razao_col and razao_col in df_viz.columns else 'cliente_nome' if 'cliente_nome' in df_viz.columns else None
        setor_col_candidates = [c for c in df_viz.columns if 'setor' in c.lower() or 'atividade' in c.lower()]
        setor_col = setor_col_candidates[0] if setor_col_candidates else None

        # preparar df apenas com clientes que são prioritários (pelo código emissor)
        # lembrando: df_prior já contém apenas clientes prioritários, usar df_prior para garantir consistência
        df_top_source = df_viz.copy()
        # contar por cliente: quantidade de pedidos prioritários e por tipo combustível
        def is_material_in_priority(mat):
            return str(mat).strip().upper() in [m.upper() for m in PRIORITY_MATERIALS]

        # assegurar coluna material e ordem_de_venda presentes
        if material_col not in df_top_source.columns:
            st.warning("Coluna material ausente — Top10 terá dados limitados.")
        if client_col not in df_top_source.columns:
            st.error("Coluna código emissor ausente — Top10 não pode ser calculado.")
        else:
            # criar tipo_combustivel baseado na função já existente (reaproveitamos a mesma heurística)
            tmp = df_top_source.copy()
            tmp['material_norm'] = tmp[material_col].astype(str).str.strip().str.upper()
            def class_comb(mat):
                m = str(mat).lower()
                if 'etan' in m:
                    return 'Etanol'
                if 'gaso' in m or 'gasol' in m:
                    return 'Gasolina'
                if 'dies' in m:
                    return 'Diesel'
                return 'Outros'
            tmp['tipo_combustivel'] = tmp['material_norm'].apply(class_comb)
            tmp['is_priority_mat'] = tmp['material_norm'].apply(lambda x: is_material_in_priority(x))

            # agregações por cliente (codigo emissor)
            agg = tmp.groupby(client_col).agg(
                cliente_nome = (cliente_name_col, 'first') if cliente_name_col and cliente_name_col in tmp.columns else (client_col, 'first'),
                qtd_prioritarios = ('is_priority_mat', 'sum'),
                qtd_etanol = ( 'tipo_combustivel', lambda s: (s.str.fullmatch('Etanol', case=False)).sum() if s.dtype == 'object' else 0),
                qtd_gasolina = ( 'tipo_combustivel', lambda s: (s.str.fullmatch('Gasolina', case=False)).sum() if s.dtype == 'object' else 0),
                qtd_diesel = ( 'tipo_combustivel', lambda s: (s.str.fullmatch('Diesel', case=False) & ~tmp['is_priority_mat']).sum() if s.dtype == 'object' else 0),
                qtd_lubrificantes = (setor_col, lambda s: (s.str.contains('Lubrificantes', case=False, na=False)).sum() if setor_col and setor_col in tmp.columns else 0),
                ov_count = ('ordem_de_venda', 'nunique')
            ).reset_index().rename(columns={client_col: 'codigo_sap'})

            # calcular OTIFs por cliente para AGRITOP / demais / lubrificantes — defensivo:
            # vamos calcular OTIF por cliente apenas quando 'otif_atendido' estiver presente
            if 'otif_atendido' in tmp.columns:
                otif_by_client = tmp.groupby(client_col).agg(otif_per_cliente = ('otif_atendido', 'mean')).reset_index().rename(columns={client_col: 'codigo_sap'})
                agg = agg.merge(otif_by_client, left_on='codigo_sap', right_on='codigo_sap', how='left')
                agg['otif_per_cliente'] = (agg['otif_per_cliente'] * 100).round(2)
            else:
                agg['otif_per_cliente'] = np.nan

            # Para OTIF AGRITOP e OTIF Lubrificantes, tentar filtrar linhas específicas
            # OTIF AGRITOP
            if 'otif_atendido' in tmp.columns:
                agr = tmp[tmp['material_norm'].isin([m.upper() for m in PRIORITY_MATERIALS])]
                agr_agg = agr.groupby(client_col).agg(otif_agr = ('otif_atendido','mean')).reset_index().rename(columns={client_col:'codigo_sap'})
                agr_agg['otif_agr'] = (agr_agg['otif_agr'] * 100).round(2)
                agg = agg.merge(agr_agg, on='codigo_sap', how='left')
            else:
                agg['otif_agr'] = np.nan

            # OTIF Lubrificantes
            if setor_col and 'otif_atendido' in tmp.columns and setor_col in tmp.columns:
                lub = tmp[tmp[setor_col].str.contains('Lubrificantes', case=False, na=False)]
                lub_agg = lub.groupby(client_col).agg(otif_lub = ('otif_atendido','mean')).reset_index().rename(columns={client_col:'codigo_sap'})
                lub_agg['otif_lub'] = (lub_agg['otif_lub'] * 100).round(2)
                agg = agg.merge(lub_agg, on='codigo_sap', how='left')
            else:
                agg['otif_lub'] = np.nan

            # OTIF demais produtos (exclui prioritários)
            if 'otif_atendido' in tmp.columns:
                nonprio = tmp[~tmp['is_priority_mat']]
                nonprio_agg = nonprio.groupby(client_col).agg(otif_nonprio = ('otif_atendido','mean')).reset_index().rename(columns={client_col:'codigo_sap'})
                nonprio_agg['otif_nonprio'] = (nonprio_agg['otif_nonprio'] * 100).round(2)
                agg = agg.merge(nonprio_agg, on='codigo_sap', how='left')
            else:
                agg['otif_nonprio'] = np.nan

            # formatar e ordenar por qtd_prioritarios
            agg['qtd_prioritarios'] = agg['qtd_prioritarios'].fillna(0).astype(int)
            agg = agg.sort_values('qtd_prioritarios', ascending=False).head(10)

            # renomear colunas finais conforme solicitado
            display_cols = {
                'codigo_sap': 'Código',
                'cliente_nome': 'Cliente',
                'qtd_prioritarios': 'AGRITOP',
                'qtd_etanol': 'Etanol',
                'qtd_gasolina': 'Gasolina',
                'qtd_diesel': 'Diesel',
                'qtd_lubrificantes': 'Lubs',
                'otif_agr': 'OTIF AGRITOP',
                'otif_nonprio': 'OTIF D. produtos',
                'otif_lub': 'OTIF Lubs'
            }
            # aplicar renome
            agg_display = agg.rename(columns=display_cols)
            cols_to_show = [c for c in display_cols.values() if c in agg_display.columns]

            # garantir col com ordenação esperada
            st.dataframe(agg_display[cols_to_show].reset_index(drop=True), use_container_width=True)

# st.markdown("---")
# st.subheader("Distribuição de Pedidos")

col1, col2 = st.columns(2)
st.markdown("---")
st.subheader("Distribuição de Pedidos")

col1, col2 = st.columns(2)

# ============================
# COLUNA 1 — PIZZA PRIORITÁRIO × DEMAIS
# ============================
with col1:
    pie1 = pd.DataFrame({
        "categoria": ["Prioritários", "Demais"],
        "quantidade": [total_pedidos_priority, total_pedidos_nonpriority]
    })

    fig1 = px.pie(
        pie1,
        names="categoria",
        values="quantidade",
        title="Prioritários × Demais"
    )
    st.plotly_chart(fig1, use_container_width=True)


# ============================
# COLUNA 2 — PIZZA ETANOL / GASOLINA / DIESEL / LUBRIFICANTES
# ============================
with col2:
    df_np = df_nonpriority.copy()

    def class_comb(m):
        m = str(m).lower()
        if "etan" in m: return "Etanol"
        if "gaso" in m: return "Gasolina"
        if "dies" in m: return "Diesel"
        return "Lubrificante"

    df_np["tipo_combustivel"] = df_np[material_col].astype(str).apply(class_comb)

    pie2 = df_np["tipo_combustivel"].value_counts().reset_index()
    pie2.columns = ["categoria", "quantidade"]

    fig2 = px.pie(
        pie2,
        names="categoria",
        values="quantidade",
        title="Distribuição — Demais Materiais"
    )
    st.plotly_chart(fig2, use_container_width=True)


    # with aba2:
    #     st.header("Visão Operacional — Produtos Claros")   

    #     # -----------------------------
    #     # KPIs gerais (clientes distintos, total OV, %OTIF) — com base no df_view
    #     # -----------------------------
    #     # Contagem de clientes prioritários (qualquer pedido de material prioritário)
    #     total_clientes_prioritarios = df_main[df_main['is_priority_material']][client_col].nunique()

    #     # Total de ordens de venda e OTIF continuam usando df_view filtrado
    #     total_ov_view = df_view['ordem_de_venda'].nunique()
    #     perc_otif_view = df_view['otif_atendido'].mean() * 100 if len(df_view) > 0 else 0

    #     # Exibir métricas
    #     kc1, kc2, kc3 = st.columns(3)
    #     kc1.metric('Clientes Agritop / Off Road', int(total_clientes_prioritarios))
    #     kc2.metric('Total de Ordens de Venda (Exceto Agritop / Off Road)', int(total_ov_view))
    #     kc3.metric('OTIF (%)', f"{perc_otif_view:,.2f}%")

    #     # -----------------------------
    #     # Filtros: Base e Diretoria N2
    #     # -----------------------------
    #     st.subheader('Filtros')
    #     f1, f2 = st.columns(2)
    #     base_col = next((c for c in df_view.columns if 'base' == c or 'base' in c.lower()), None)
    #     dir_n2_col = n2_col

    #     sel_base = None
    #     sel_n2 = None
    #     df_filtered = df_view.copy()
    #     if base_col is not None:
    #         sel_base = f1.multiselect('Base', options=sorted(df_view[base_col].dropna().unique().tolist()), default=None)
    #         if sel_base:
    #             df_filtered = df_filtered[df_filtered[base_col].isin(sel_base)]
    #     if dir_n2_col is not None:
    #         sel_n2 = f2.multiselect('Diretoria N2', options=sorted(df_view[dir_n2_col].dropna().unique().tolist()), default=None)
    #         if sel_n2:
    #             df_filtered = df_filtered[df_filtered[dir_n2_col].isin(sel_n2)]

    #     # -----------------------------
    #     # Tabela final com colunas solicitadas
    #     # -----------------------------
    #     st.subheader('Tabela filtrada — Prioritários (Exceto Agritop / Off Road)')

    #     cols_to_show = [client_col]
    #     if razao_col:
    #         cols_to_show.append(razao_col)
    #     cols_to_show += ['ordem_de_venda', material_col, 'status_check']

    #     # proteger se colausente
    #     cols_to_show = [c for c in cols_to_show if c in df_filtered.columns]

    #     st.dataframe(df_filtered[cols_to_show].drop_duplicates().reset_index(drop=True), use_container_width=True)

    #     # ----------------------------
    #     # Distribuição por status (global)
    #     # ----------------------------
    #     st.subheader('Distribuição por Status Check')
    #     fig_status = px.histogram(df_prior, x='status_check', title='Status dos pedidos', labels={'status_check': 'Status'}, text_auto=True, color_discrete_sequence=[COLORS['verde_escuro']])
    #     st.plotly_chart(fig_status, use_container_width=True)

    #     # ----------------------------
    #     # Tabela completa e Export
    #     # ----------------------------
    #     st.subheader('Tabela filtrada — Prioritários (com filtros aplicáveis)')
    #     statuses = df_prior['status_check'].dropna().unique().tolist()
    #     sel_status = st.multiselect('Status', options=sorted(statuses), default=sorted(statuses))
    #     sel_base_tbl = None
    #     if 'base' in df_prior.columns:
    #         sel_base_tbl = st.multiselect('Base (tabela)', options=sorted(df_prior['base'].dropna().unique().tolist()), default=None)

    #     # preparar df_view for table

    #     df_table = df_prior.copy()
    #     if sel_status:
    #         df_table = df_table[df_table['status_check'].isin(sel_status)]
    #     if sel_base_tbl:
    #         df_table = df_table[df_table['base'].isin(sel_base_tbl)]

    #     default_show = [client_col, 'ordem_de_venda', material_col, 'status_check']
    #     show_cols = st.multiselect('Colunas a exibir', options=df_table.columns.tolist(), default=[c for c in default_show if c in df_table.columns])
    #     st.dataframe(df_table[show_cols].reset_index(drop=True), use_container_width=True)

    #     # Export
    #     st.subheader('Exportar resultados')
    #     if st.button('Exportar XLSX por Código SAP (gera arquivos em ./exports/<YYYY-MM-DD>/)'):
    #         written = export_by_sapcode(df_table, sap_col if sap_col in df_table.columns else 'ordem_de_venda')
    #         if written:
    #             st.success(f"{len(written)} arquivos gerados.")
    #             zip_buf = make_zip(written)
    #             st.download_button('Baixar ZIP dos arquivos exportados', data=zip_buf, file_name=f"exports_{datetime.now().strftime('%Y%m%d')}.zip", mime='application/zip')
    #         else:
    #             st.warning('Nenhum arquivo foi escrito.')

    #     # download consolidado
    #     to_xlsx = io.BytesIO()
    #     with pd.ExcelWriter(to_xlsx, engine='openpyxl') as writer:
    #         df_table.to_excel(writer, sheet_name='prioritarios', index=False)
    #     to_xlsx.seek(0)
    #     st.download_button('Baixar planilha consolidada (XLSX)', data=to_xlsx, file_name='prioritarios_consolidados.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    # st.markdown("""
    # **Observações**
    # - Filtra apenas clientes que compraram VIBRA AGRITOP ou Vibra Diesel Off-Road (clientes prioritários).
    # - Dentro da visão gerencial, removemos esses materiais para analisar os demais pedidos desses clientes (Etanol/Gasolina/Diesel).
    # """)






