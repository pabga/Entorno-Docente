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


@st.cache_data(ttl=60)
def load_data_online():
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        sh = gc.open_by_url(st.secrets["cursos_sheet_url"])

        # Función auxiliar para leer hoja y limpiar encabezados
        def get_clean_df(sheet_name):
            ws = sh.worksheet(sheet_name)
            raw = ws.get_all_values()
            if not raw: return pd.DataFrame()

            # Asumimos fila 1 encabezados
            headers = [str(h).strip().upper() for h in raw[0]]
            data = raw[1:]

            # Crear DF
            df = pd.DataFrame(data, columns=headers) if data else pd.DataFrame(columns=headers)
            return df

        df_al = get_clean_df("alumnos")
        df_cu = get_clean_df("cursos")
        df_in = get_clean_df("instructores")

        # --- CARGA BLINDADA DE NOTAS ---
        ws_notas = sh.worksheet("notas")
        raw_notas = ws_notas.get_all_values()

        if not raw_notas:
            st.error("⚠️ La hoja 'notas' está vacía.")
            return df_al, df_cu, pd.DataFrame(), df_in

        headers = [str(h).strip().upper() for h in raw_notas[0]]
        data = raw_notas[1:]

        df_no = pd.DataFrame(data, columns=headers)

        # === REBAUTIZADO POR FUERZA BRUTA (POSICIÓN) ===
        # No importa cómo se llamen, forzamos los nombres clave
        cols = list(df_no.columns)
        if len(cols) >= 2:
            cols[0] = 'DNI'  # Columna A siempre es DNI
            cols[1] = 'ID_CURSO'  # Columna B siempre es ID_CURSO
            df_no.columns = cols
        else:
            st.error("⚠️ La hoja 'notas' tiene menos de 2 columnas. Debe tener al menos DNI y ID_CURSO.")
            return df_al, df_cu, pd.DataFrame(), df_in

        # Agregar índice para guardado
        df_no['ROW_INDEX'] = [i + 2 for i in range(len(data))]

        # Detectar columnas de notas dinámicamente
        all_cols = [c for c in df_no.columns if c != 'ROW_INDEX']

        # Buscamos 'COMENTARIO' o usamos todo lo demás
        idx_coment = -1
        for i, col_name in enumerate(all_cols):
            if "COMENTARIO" in col_name:
                idx_coment = i
                break

        if idx_coment != -1:
            # Notas están entre ID_CURSO (idx 1) y Comentarios
            notas_cols = all_cols[2:idx_coment]
        else:
            notas_cols = all_cols[2:]  # Si no hay comentarios, todo el resto son notas

        # Convertir a números
        for c in notas_cols:
            df_no[c] = pd.to_numeric(df_no[c], errors='coerce').fillna(0)

        st.session_state['notas_header_list'] = notas_cols
        st.session_state['full_header_list'] = df_no.columns.tolist()

        return df_al, df_cu, df_no, df_in

    except Exception as e:
        st.error(f"❌ Error crítico en carga: {e}")
        return None, None, None, None


def sincronizar_matriz_notas(df_alumnos, df_cursos, df_notas_actuales):
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        ws_notas = gc.open_by_url(st.secrets["cursos_sheet_url"]).worksheet("notas")

        # Filtramos vacíos
        alumnos_dni = [str(d).strip() for d in df_alumnos['DNI'].unique() if str(d).strip()]
        cursos_id = [str(c).strip().upper() for c in df_cursos['D_CURSO'].unique() if str(c).strip()]

        existentes = set()
        if not df_notas_actuales.empty and 'DNI' in df_notas_actuales.columns:
            # Usamos iloc para asegurar que leemos las col 0 y 1 aunque tengan mal nombre
            dnis = df_notas_actuales.iloc[:, 0].astype(str).str.strip()
            curso_ids = df_notas_actuales.iloc[:, 1].astype(str).str.strip().upper()
            existentes = set(zip(dnis, curso_ids))

        nuevas_filas = []
        # Calculamos cuántas columnas vacías agregar
        total_cols_reales = len(st.session_state.get('full_header_list', []))
        # Si la hoja está vacía, asumimos 5 columnas base
        padding = total_cols_reales - 2 if total_cols_reales > 2 else 3

        for c in cursos_id:
            for a in alumnos_dni:
                if (a, c) not in existentes:
                    fila = [a, c] + [""] * padding
                    nuevas_filas.append(fila)

        if nuevas_filas:
            ws_notas.append_rows(nuevas_filas)
            st.success(f"✅ Sincronización: {len(nuevas_filas)} registros creados.")
            st.cache_data.clear()
            st.rerun()
        else:
            st.info("Todo al día.")
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
        # Lógica de guardado
        cols_notas = st.session_state.get('notas_header_list', [])
        col_coment = next((c for c in edited_df.columns if "COMENTARIO" in c), None)
        cols_save = cols_notas + ([col_coment] if col_coment else [])

        for i in range(len(edited_df)):
            fila = edited_df.iloc[i]
            gs_row = int(fila['ROW_INDEX'])

            for col in cols_save:
                # Verificamos que la columna exista tanto en el editor como en los headers originales
                if col in fila and col in headers:
                    val = fila[col]
                    if hasattr(val, "item"): val = val.item()
                    if pd.isna(val): val = ""

                    col_gs = headers.index(col) + 1
                    batch.append({'range': utils.rowcol_to_a1(gs_row, col_gs), 'values': [[str(val)]]})

        if batch:
            ws.batch_update(batch)
            st.success("✅ Guardado.")
            st.cache_data.clear()
            st.rerun()
    except Exception as e:
        st.error(f"Error Guardar: {e}")


def procesar_datos(df_al, df_cu, df_no):
    # Verificación defensiva
    if df_no is None or df_no.empty:
        return pd.DataFrame()

    # Renombrado forzoso para asegurar el cruce
    # (Ya se hizo en load_data_online, pero aseguramos tipos)
    df_no['DNI'] = df_no['DNI'].astype(str).str.strip()
    df_no['ID_CURSO'] = df_no['ID_CURSO'].astype(str).str.strip().upper()

    df_al['DNI'] = df_al['DNI'].astype(str).str.strip()

    # Check si en cursos existe D_CURSO, si no, intentamos la 1ra columna
    if 'D_CURSO' not in df_cu.columns and len(df_cu.columns) > 0:
        df_cu.rename(columns={df_cu.columns[0]: 'D_CURSO'}, inplace=True)

    df_cu['D_CURSO'] = df_cu['D_CURSO'].astype(str).str.strip().upper()

    # Cruces
    if 'NOMBRE' in df_al.columns and 'APELLIDO' in df_al.columns:
        df = pd.merge(df_no, df_al[['DNI', 'NOMBRE', 'APELLIDO']], on='DNI', how='left')
    else:
        # Si no encuentra nombre/apellido, al menos muestra el DNI
        df = df_no.copy()
        df['NOMBRE'] = ""
        df['APELLIDO'] = ""

    if 'ASIGNATURA' in df_cu.columns:
        df = pd.merge(df, df_cu[['D_CURSO', 'ASIGNATURA']], left_on='ID_CURSO', right_on='D_CURSO', how='left')
    else:
        df['ASIGNATURA'] = df['ID_CURSO']

    def get_principal(x):
        return x.split('-')[0] if '-' in str(x) else str(x)

    df[CURSO_PRINCIPAL] = df['ID_CURSO'].apply(get_principal)
    return df


# --- INTERFAZ UI ---
st.set_page_config(page_title="Sistema Notas", layout="wide")

# Carga
df_al, df_cu, df_no, df_in = load_data_online()

# Proceso seguro (si falla la carga, df_no es vacio)
if df_no is not None and not df_no.empty:
    df_final = procesar_datos(df_al, df_cu, df_no)
else:
    df_final = pd.DataFrame()

# Sesión
if 'auth' not in st.session_state: st.session_state.auth = False

if not st.session_state.auth:
    st.title("Acceso")
    u = st.text_input("DNI")
    p = st.text_input("Clave", type="password")
    if st.button("Ingresar"):
        # Intenta buscar credenciales
        if df_in is not None and not df_in.empty:
            # Forzar nombres col 0 y 2 si es necesario
            cols_in = df_in.columns.tolist()
            if 'DNI_DOCENTE' not in cols_in and len(cols_in) > 0:
                df_in.rename(columns={cols_in[0]: 'DNI_DOCENTE'}, inplace=True)
            if 'CLAVE_ACCESO' not in cols_in and len(cols_in) > 2:  # Asumiendo col 3 es clave
                # Buscamos col que parezca clave o usamos la 3ra
                pass

            # Búsqueda flexible
            try:
                found = df_in[(df_in.iloc[:, 0].astype(str) == u) & (df_in.iloc[:, 2].astype(str) == p)]
                if not found.empty:
                    st.session_state.auth = True
                    st.session_state.dni = u
                    # Asumiendo ID_CURSO en col 1
                    st.session_state.cursos = [str(x).upper().strip() for x in found.iloc[:, 1].tolist()]
                    st.rerun()
                else:
                    st.error("Incorrecto")
            except:
                st.error("Error validando usuario. Revise hoja Instructores.")
        else:
            st.error("No se pudo cargar la lista de instructores.")
else:
    sb = st.sidebar
    sb.write(f"Usuario: {st.session_state.dni}")

    if str(st.session_state.dni) == DNI_ADMIN:
        sb.info("Modo Admin")
        if sb.button("Sincronizar Alumnos"):
            sincronizar_matriz_notas(df_al, df_cu, df_no)

    if sb.button("Salir"):
        st.session_state.auth = False
        st.rerun()

    if df_final.empty:
        st.warning("No hay datos de notas disponibles.")
    else:
        mis = st.session_state.cursos
        df_mio = df_final[df_final['ID_CURSO'].isin(mis)]

        if df_mio.empty:
            st.warning("No tienes cursos asignados o no hay alumnos en ellos.")
        else:
            grps = sorted(df_mio[CURSO_PRINCIPAL].unique())
            tabs = st.tabs(grps)
            for i, g in enumerate(grps):
                with tabs[i]:
                    dft = df_mio[df_mio[CURSO_PRINCIPAL] == g].reset_index(drop=True)

                    col_com = next((c for c in dft.columns if "COMENTARIO" in c), None)
                    cols_n = st.session_state.get('notas_header_list', [])

                    cols_view = ['NOMBRE', 'APELLIDO', 'ID_CURSO'] + cols_n + ([col_com] if col_com else [])
                    cols_edit = ['ROW_INDEX'] + cols_view

                    # Filtramos solo columnas que realmente existan en el DF procesado
                    cols_edit = [c for c in cols_edit if c in dft.columns]

                    cnf = {k: st.column_config.Column(disabled=True) for k in
                           ['ROW_INDEX', 'NOMBRE', 'APELLIDO', 'ID_CURSO']}

                    out = st.data_editor(dft[cols_edit], column_config=cnf, key=f"e_{g}", hide_index=True,
                                         use_container_width=True)

                    if st.button(f"Guardar {g}", key=f"b_{g}"):
                        guardar_cambios(out)