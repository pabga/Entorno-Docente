import streamlit as st
import pandas as pd
import gspread
import numpy as np
import re
from gspread import utils

# --- CONFIGURACIÓN ---
DNI_ADMIN = "41209872"
CURSO_PRINCIPAL = 'CURSO_PRINCIPAL'


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


@st.cache_data(ttl=60)  # Cache bajo para depuración
def load_data_online():
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        sh = gc.open_by_url(st.secrets["cursos_sheet_url"])

        # Función que carga datos crudos y fuerza encabezados
        def get_clean_df(sheet_name):
            ws = sh.worksheet(sheet_name)
            raw_data = ws.get_all_values()  # Matriz pura
            if not raw_data: return pd.DataFrame()

            headers = [str(h).strip().upper() for h in raw_data[0]]
            data = raw_data[1:]

            if not data: return pd.DataFrame(columns=headers)

            df = pd.DataFrame(data, columns=headers)
            return df

        df_al = get_clean_df("alumnos")
        df_cu = get_clean_df("cursos")
        df_in = get_clean_df("instructores")

        # --- CARGA ESPECIAL PARA NOTAS ---
        ws_notas = sh.worksheet("notas")
        raw_notas = ws_notas.get_all_values()

        if not raw_notas or len(raw_notas) < 1:
            st.error("⚠️ La hoja 'notas' está totalmente vacía.")
            return df_al, df_cu, pd.DataFrame(), df_in

        # Encabezados brutos
        headers = [str(h).strip().upper() for h in raw_notas[0]]

        # --- CORRECCIÓN FORZADA DE ENCABEZADOS POR POSICIÓN ---
        # Si la columna A no se llama DNI, la renombramos a la fuerza
        if len(headers) > 0: headers[0] = 'DNI'
        # Si la columna B no se llama ID_CURSO, la renombramos a la fuerza
        if len(headers) > 1: headers[1] = 'ID_CURSO'

        df_no = pd.DataFrame(raw_notas[1:], columns=headers)

        # Agregamos índice de fila para guardar
        df_no['ROW_INDEX'] = [i + 2 for i in range(len(df_no))]

        # Detectar columnas de notas (Todo lo que está entre la col 2 y 'COMENTARIOS')
        all_cols = [c for c in df_no.columns if c != 'ROW_INDEX']

        # Búsqueda de columna comentario
        idx_coment = -1
        for i, col in enumerate(all_cols):
            if "COMENTARIO" in col:
                idx_coment = i
                break

        if idx_coment != -1:
            notas_cols = all_cols[2:idx_coment]  # Desde Columna C hasta Comentarios
        else:
            notas_cols = all_cols[2:]  # Si no hay comentarios, todo el resto son notas

        # Convertir a números
        for c in notas_cols:
            df_no[c] = pd.to_numeric(df_no[c], errors='coerce').fillna(0)

        st.session_state['notas_header_list'] = notas_cols
        st.session_state['full_header_list'] = df_no.columns.tolist()  # Lista corregida

        return df_al, df_cu, df_no, df_in

    except Exception as e:
        st.error(f"❌ Error crítico cargando datos: {e}")
        return None, None, None, None


def sincronizar_matriz_notas(df_alumnos, df_cursos, df_notas_actuales):
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        ws_notas = gc.open_by_url(st.secrets["cursos_sheet_url"]).worksheet("notas")

        alumnos_dni = [str(d).strip() for d in df_alumnos['DNI'].unique() if d]
        cursos_id = [str(c).strip().upper() for c in df_cursos['D_CURSO'].unique() if c]

        existentes = set()
        if not df_notas_actuales.empty and 'DNI' in df_notas_actuales.columns:
            existentes = set(zip(df_notas_actuales['DNI'].astype(str).str.strip(),
                                 df_notas_actuales['ID_CURSO'].astype(str).str.strip().upper()))

        nuevas_filas = []
        # Preparamos filas con el ancho correcto
        total_cols = len(st.session_state.get('full_header_list', []))
        if total_cols < 2: total_cols = 5  # Fallback

        for c in cursos_id:
            for a in alumnos_dni:
                if (a, c) not in existentes:
                    # DNI, ID_CURSO, y resto vacíos
                    fila = [a, c] + [""] * (total_cols - 2)
                    nuevas_filas.append(fila)

        if nuevas_filas:
            ws_notas.append_rows(nuevas_filas)
            st.success(f"✅ Sincronización completa: {len(nuevas_filas)} registros creados.")
            st.cache_data.clear()
            st.rerun()
        else:
            st.info("La base de datos ya está actualizada.")
    except Exception as e:
        st.error(f"Error Sync: {e}")


def guardar_cambios(edited_df):
    if edited_df is None or edited_df.empty: return
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        ws = gc.open_by_url(st.secrets["cursos_sheet_url"]).worksheet("notas")
        headers = st.session_state['full_header_list']

        batch = []
        # Buscamos dinámicamente qué columnas guardar
        cols_to_check = st.session_state.get('notas_header_list', [])
        col_coment = next((c for c in edited_df.columns if "COMENTARIO" in c), None)
        if col_coment: cols_to_check.append(col_coment)

        for i in range(len(edited_df)):
            fila = edited_df.iloc[i]
            gs_row = int(fila['ROW_INDEX'])

            for col in cols_to_check:
                if col in fila and col in headers:
                    val = fila[col]
                    if hasattr(val, "item"): val = val.item()  # Fix int64 error
                    if pd.isna(val): val = ""

                    # Encontrar índice de columna basado en la lista corregida
                    col_gs = headers.index(col) + 1
                    batch.append({'range': utils.rowcol_to_a1(gs_row, col_gs), 'values': [[str(val)]]})

        if batch:
            ws.batch_update(batch)
            st.success("✅ Datos guardados.")
            st.cache_data.clear()
            st.rerun()
    except Exception as e:
        st.error(f"Error Guardar: {e}")


def procesar_datos(df_al, df_cu, df_no):
    # --- DEPURACIÓN VISUAL SI FALLA ---
    if df_no is None or df_no.empty:
        st.warning("El DataFrame de notas está vacío.")
        return pd.DataFrame()

    # Verificación final de columnas
    if 'ID_CURSO' not in df_no.columns:
        st.error(
            f"🚨 ERROR FATAL: Incluso forzando la posición, no veo 'ID_CURSO'. Columnas actuales: {df_no.columns.tolist()}")
        st.stop()

    # Conversión segura
    df_no['DNI'] = df_no['DNI'].astype(str).str.strip()
    df_no['ID_CURSO'] = df_no['ID_CURSO'].astype(str).str.strip().upper()

    # Cruces
    df = pd.merge(df_no, df_al[['DNI', 'NOMBRE', 'APELLIDO']], on='DNI', how='left')
    df = pd.merge(df, df_cu[['D_CURSO', 'ASIGNATURA']], left_on='ID_CURSO', right_on='D_CURSO', how='left')

    def get_principal(x):
        return x.split('-')[0] if '-' in str(x) else str(x)

    df[CURSO_PRINCIPAL] = df['ID_CURSO'].apply(get_principal)
    return df


# --- INTERFAZ PRINCIPAL ---
st.set_page_config(page_title="Sistema Notas", layout="wide")
df_al, df_cu, df_no, df_in = load_data_online()

if df_no is not None:
    df_final = procesar_datos(df_al, df_cu, df_no)
else:
    df_final = pd.DataFrame()

# Login State
if 'auth' not in st.session_state: st.session_state.auth = False

if not st.session_state.auth:
    st.title("Acceso")
    u = st.text_input("DNI")
    p = st.text_input("Clave", type="password")
    if st.button("Ingresar"):
        found = df_in[(df_in['DNI_DOCENTE'].astype(str) == u) & (df_in['CLAVE_ACCESO'].astype(str) == p)]
        if not found.empty:
            st.session_state.auth = True
            st.session_state.dni = u
            st.session_state.cursos = [str(x).upper().strip() for x in found['ID_CURSO'].tolist()]
            st.rerun()
        else:
            st.error("Incorrecto")
else:
    sb = st.sidebar
    sb.write(f"Usuario: {st.session_state.dni}")

    if str(st.session_state.dni) == DNI_ADMIN:
        sb.info("Modo Admin")
        if sb.button("🔄 Sincronizar Alumnos"):
            sincronizar_matriz_notas(df_al, df_cu, df_no)

    if sb.button("Salir"):
        st.session_state.auth = False
        st.rerun()

    if df_final.empty:
        st.warning("No hay datos para mostrar.")
    else:
        mis = st.session_state.cursos
        # Filtramos
        df_mio = df_final[df_final['ID_CURSO'].isin(mis)]

        # Obtenemos grupos únicos (PCA, PPH)
        grps = sorted(df_mio[CURSO_PRINCIPAL].unique())

        if not grps:
            st.warning("No tienes cursos asignados o no coinciden con la hoja de notas.")
        else:
            tabs = st.tabs(grps)
            for i, g in enumerate(grps):
                with tabs[i]:
                    dft = df_mio[df_mio[CURSO_PRINCIPAL] == g].reset_index(drop=True)

                    # Columnas para editar
                    col_com = next((c for c in dft.columns if "COMENTARIO" in c), None)
                    cols_n = st.session_state.get('notas_header_list', [])

                    cols_view = ['NOMBRE', 'APELLIDO', 'ID_CURSO'] + cols_n + ([col_com] if col_com else [])
                    cols_edit = ['ROW_INDEX'] + cols_view

                    # Configuración de bloqueo
                    cnf = {k: st.column_config.Column(disabled=True) for k in
                           ['ROW_INDEX', 'NOMBRE', 'APELLIDO', 'ID_CURSO']}

                    out = st.data_editor(dft[cols_edit], column_config=cnf, key=f"e_{g}", hide_index=True,
                                         use_container_width=True)

                    if st.button(f"Guardar {g}", key=f"b_{g}"):
                        guardar_cambios(out)