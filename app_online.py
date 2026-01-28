import streamlit as st
import pandas as pd
import gspread
import numpy as np
import re
from gspread import utils

# --- CONFIGURACIÓN ---
DNI_ADMIN = "41209872"
CURSO_PRINCIPAL = 'CURSO_PRINCIPAL'


# --- CREDENCIALES ---
def get_gcp_credentials():
    return {
        "type": st.secrets["gcp_service_account_type"],
        "project_id": st.secrets["gcp_service_account_project_id"],
        "private_key_id": st.secrets["gcp_service_account_private_key_id"],
        "private_key": st.secrets["gcp_service_account_private_key"],
        "client_email": st.secrets["gcp_service_account_client_email"],
        "client_id": st.secrets["gcp_service_account_client_id"],
        "auth_uri": st.secrets["gcp_service_account_auth_uri"],
        "token_uri": st.secrets["gcp_service_account_token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_service_account_auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_service_account_client_x509_cert_url"],
        "universe_domain": st.secrets.get("gcp_service_account_universe_domain", "googleapis.com")
    }


# --- CARGA DE DATOS ---
def load_data_online():
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        sh = gc.open_by_url(st.secrets["cursos_sheet_url"])

        # Helper para carga segura
        def get_clean_df(sheet_name):
            try:
                ws = sh.worksheet(sheet_name)
                vals = ws.get_all_values()
                if not vals: return pd.DataFrame()
                # Encabezados en mayúsculas y limpios
                headers = [str(h).strip().upper() for h in vals[0]]
                if len(vals) > 1:
                    return pd.DataFrame(vals[1:], columns=headers)
                return pd.DataFrame(columns=headers)
            except:
                return pd.DataFrame()

        df_al = get_clean_df("alumnos")
        df_cu = get_clean_df("cursos")
        df_in = get_clean_df("instructores")

        # --- CARGA NOTAS ---
        ws_notas = sh.worksheet("notas")
        raw_notas = ws_notas.get_all_values()

        if not raw_notas:
            # Si está vacía, devolvemos DF vacío pero con columnas base para que no explote
            return df_al, df_cu, pd.DataFrame(columns=['DNI', 'ID_CURSO', 'ROW_INDEX']), df_in

        # Creamos DF con datos crudos
        headers = [str(h).strip().upper() for h in raw_notas[0]]
        data = raw_notas[1:]
        df_no = pd.DataFrame(data, columns=headers)

        # Agregar índice de fila (crítico para guardar)
        df_no['ROW_INDEX'] = [i + 2 for i in range(len(data))]

        return df_al, df_cu, df_no, df_in

    except Exception as e:
        st.error(f"❌ Error conectando a Google Sheets: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# --- PROCESAMIENTO BLINDADO (AQUÍ ESTÁ LA SOLUCIÓN) ---
def procesar_datos(df_al, df_cu, df_no):
    # 1. Chequeo de vacío
    if df_no is None or df_no.empty:
        return pd.DataFrame()

    # 2. RENOMBRADO FORZOSO POR POSICIÓN
    # Obtenemos lista de columnas actuales
    cols = list(df_no.columns)

    # Nos aseguramos que ROW_INDEX no se toque (suele ser la última si la agregamos antes)
    # Buscamos columnas de datos reales
    data_cols = [c for c in cols if c != 'ROW_INDEX']

    # Lógica de fuerza bruta:
    # La 1ra columna de datos es DNI. La 2da es ID_CURSO.
    mapa_nuevos_nombres = {}

    if len(data_cols) >= 1:
        mapa_nuevos_nombres[data_cols[0]] = 'DNI'
    if len(data_cols) >= 2:
        mapa_nuevos_nombres[data_cols[1]] = 'ID_CURSO'

    # Aplicamos el renombrado
    df_no.rename(columns=mapa_nuevos_nombres, inplace=True)

    # 3. VERIFICACIÓN DE SUPERVIVENCIA
    # Si después de renombrar no tenemos ID_CURSO (porque solo había 1 columna), la creamos
    if 'ID_CURSO' not in df_no.columns:
        df_no['ID_CURSO'] = "SIN_ID"
    if 'DNI' not in df_no.columns:
        df_no['DNI'] = "SIN_DNI"

    # 4. AHORA ES SEGURO OPERAR
    df_no['DNI'] = df_no['DNI'].astype(str).str.strip()
    df_no['ID_CURSO'] = df_no['ID_CURSO'].astype(str).str.strip().upper()

    # Detectar dinámicamente columnas de notas para visualización
    # Asumimos que todo lo que no sea DNI, ID_CURSO, ROW_INDEX son notas/comentarios
    cols_extra = [c for c in df_no.columns if c not in ['DNI', 'ID_CURSO', 'ROW_INDEX']]

    # Separar notas de comentarios
    notas_cols = []
    col_comentarios = None

    for c in cols_extra:
        if "COMENTARIO" in c:
            col_comentarios = c
            break
        else:
            # Convertir a numérico solo si parece nota
            df_no[c] = pd.to_numeric(df_no[c], errors='coerce').fillna(0)
            notas_cols.append(c)

    # Guardar en sesión para usar en la UI
    st.session_state['notas_header_list'] = notas_cols
    st.session_state['col_comentarios'] = col_comentarios
    st.session_state['full_header_list'] = df_no.columns.tolist()

    # 5. Normalizar DF auxiliares para cruce
    if not df_al.empty:
        # Forzar col 0 a DNI
        df_al.rename(columns={df_al.columns[0]: 'DNI'}, inplace=True)
        df_al['DNI'] = df_al['DNI'].astype(str).str.strip()

    if not df_cu.empty:
        # Forzar col 0 a D_CURSO
        df_cu.rename(columns={df_cu.columns[0]: 'D_CURSO'}, inplace=True)
        df_cu['D_CURSO'] = df_cu['D_CURSO'].astype(str).str.strip().upper()

    # 6. CRUCES (MERGES)
    # Cruce seguro con Alumnos
    if 'NOMBRE' in df_al.columns and 'APELLIDO' in df_al.columns:
        df = pd.merge(df_no, df_al[['DNI', 'NOMBRE', 'APELLIDO']], on='DNI', how='left')
    else:
        df = df_no.copy()
        df['NOMBRE'] = "---"
        df['APELLIDO'] = "---"

    # Cruce seguro con Cursos
    if 'ASIGNATURA' in df_cu.columns:
        df = pd.merge(df, df_cu[['D_CURSO', 'ASIGNATURA']], left_on='ID_CURSO', right_on='D_CURSO', how='left')
    else:
        df['ASIGNATURA'] = df['ID_CURSO']

    # Columna Principal
    df[CURSO_PRINCIPAL] = df['ID_CURSO'].apply(lambda x: x.split('-')[0] if '-' in str(x) else str(x))

    return df


# --- SINCRONIZACIÓN ---
def sincronizar_matriz_notas(df_alumnos, df_cursos, df_notas_actuales):
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        ws_notas = gc.open_by_url(st.secrets["cursos_sheet_url"]).worksheet("notas")

        # Datos fuente
        dnis = []
        if not df_alumnos.empty:
            dnis = [str(x).strip() for x in df_alumnos.iloc[:, 0].unique() if str(x).strip()]

        cursos = []
        if not df_cursos.empty:
            cursos = [str(x).strip().upper() for x in df_cursos.iloc[:, 0].unique() if str(x).strip()]

        # Existentes
        existentes = set()
        if not df_notas_actuales.empty and 'DNI' in df_notas_actuales.columns and 'ID_CURSO' in df_notas_actuales.columns:
            ex_dnis = df_notas_actuales['DNI'].astype(str).str.strip()
            ex_curs = df_notas_actuales['ID_CURSO'].astype(str).str.strip().upper()
            existentes = set(zip(ex_dnis, ex_curs))

        nuevas = []
        # Calcular columnas de relleno (padding)
        # Necesitamos llenar hasta el ancho actual del excel.
        # Si está vacío, asumimos al menos 3 columnas vacías para notas.
        num_cols_actuales = len(st.session_state.get('full_header_list', []))
        padding = num_cols_actuales - 3  # Restamos DNI, ID_CURSO, ROW_INDEX
        if padding < 1: padding = 5

        for c in cursos:
            for a in dnis:
                if (a, c) not in existentes:
                    nuevas.append([a, c] + [""] * padding)

        if nuevas:
            ws_notas.append_rows(nuevas)
            st.success(f"✅ Sincronizados {len(nuevas)} registros.")
            st.rerun()
        else:
            st.info("Todo al día.")
    except Exception as e:
        st.error(f"Error Sync: {e}")


# --- GUARDADO ---
def guardar_cambios(edited_df):
    if edited_df is None or edited_df.empty: return
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        ws = gc.open_by_url(st.secrets["cursos_sheet_url"]).worksheet("notas")

        # Recuperar nombres reales de columnas para saber índice
        headers = st.session_state['full_header_list']
        # Quitamos ROW_INDEX de la lista de headers reales de google sheets
        gs_headers = [h for h in headers if h != 'ROW_INDEX']

        batch = []

        # Columnas a guardar: Notas + Comentarios
        cols_to_save = st.session_state.get('notas_header_list', [])
        col_com = st.session_state.get('col_comentarios')
        if col_com: cols_to_save.append(col_com)

        for i in range(len(edited_df)):
            fila = edited_df.iloc[i]
            if 'ROW_INDEX' not in fila: continue

            gs_row = int(fila['ROW_INDEX'])

            for col in cols_to_save:
                if col in fila and col in gs_headers:
                    val = fila[col]
                    if hasattr(val, "item"): val = val.item()
                    if pd.isna(val): val = ""

                    # Indice 1-based
                    col_idx = gs_headers.index(col) + 1
                    batch.append({'range': utils.rowcol_to_a1(gs_row, col_idx), 'values': [[str(val)]]})

        if batch:
            ws.batch_update(batch)
            st.success("✅ Guardado.")
            st.rerun()
    except Exception as e:
        st.error(f"Error Guardar: {e}")


# --- INTERFAZ ---
st.set_page_config(page_title="Gestión Notas", layout="wide")

df_al, df_cu, df_no, df_in = load_data_online()
df_final = procesar_datos(df_al, df_cu, df_no)

if 'auth' not in st.session_state: st.session_state.auth = False

if not st.session_state.auth:
    st.title("Acceso")
    u = st.text_input("DNI")
    p = st.text_input("Clave", type="password")
    if st.button("Entrar"):
        if not df_in.empty:
            # Login por posición (0=DNI, 2=Clave)
            try:
                # Limpiar
                df_in.rename(columns={df_in.columns[0]: 'USER_DNI'}, inplace=True)
                if len(df_in.columns) > 2:
                    df_in.rename(columns={df_in.columns[2]: 'USER_PASS'}, inplace=True)
                    df_in.rename(columns={df_in.columns[1]: 'USER_CURSO'}, inplace=True)

                match = df_in[
                    (df_in.iloc[:, 0].astype(str).str.strip() == u) & (df_in.iloc[:, 2].astype(str).str.strip() == p)]

                if not match.empty:
                    st.session_state.auth = True
                    st.session_state.dni = u
                    st.session_state.cursos = [str(x).strip().upper() for x in match.iloc[:, 1].unique()]
                    st.rerun()
                else:
                    st.error("Incorrecto")
            except:
                st.error("Error validando.")
else:
    sb = st.sidebar
    sb.title(f"Usuario: {st.session_state.dni}")

    if str(st.session_state.dni) == DNI_ADMIN:
        sb.info("Admin")
        if sb.button("Sincronizar Alumnos"):
            sincronizar_matriz_notas(df_al, df_cu, df_no)

    if sb.button("Salir"):
        st.session_state.auth = False
        st.rerun()

    if df_final.empty:
        st.warning("No hay notas cargadas. Si es Admin, use Sincronizar.")
    else:
        mis = st.session_state.cursos
        df_mio = df_final[df_final['ID_CURSO'].isin(mis)]

        if df_mio.empty:
            st.warning("No tienes cursos con alumnos asignados.")
        else:
            grps = sorted(df_mio[CURSO_PRINCIPAL].unique())
            tabs = st.tabs(grps)
            for i, g in enumerate(grps):
                with tabs[i]:
                    dft = df_mio[df_mio[CURSO_PRINCIPAL] == g].reset_index(drop=True)

                    cols_n = st.session_state.get('notas_header_list', [])
                    col_c = st.session_state.get('col_comentarios')

                    view_cols = ['NOMBRE', 'APELLIDO', 'ID_CURSO'] + cols_n + ([col_c] if col_c else [])
                    # Solo columnas que existen
                    view_cols = [c for c in view_cols if c in dft.columns]
                    edit_cols = ['ROW_INDEX'] + view_cols

                    cnf = {k: st.column_config.Column(disabled=True) for k in
                           ['ROW_INDEX', 'NOMBRE', 'APELLIDO', 'ID_CURSO']}

                    out = st.data_editor(dft[edit_cols], column_config=cnf, key=f"e_{g}", hide_index=True,
                                         use_container_width=True)

                    if st.button(f"Guardar {g}", key=f"b_{g}"):
                        guardar_cambios(out)