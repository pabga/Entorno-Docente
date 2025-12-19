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


@st.cache_data(ttl=300)
def load_data_online():
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        sh = gc.open_by_url(st.secrets["cursos_sheet_url"])

        # Función para cargar y limpiar encabezados de forma forzada
        def get_cleaned_df(sheet_name):
            ws = sh.worksheet(sheet_name)
            data = ws.get_all_records()
            if not data:
                return pd.DataFrame()
            df = pd.DataFrame(data)
            df.columns = [str(c).strip().upper() for c in df.columns]
            return df

        df_al = get_cleaned_df("alumnos")
        df_cu = get_cleaned_df("cursos")
        df_in = get_cleaned_df("instructores")

        # Carga especial para notas (necesitamos ROW_INDEX para no pisar datos)
        ws_notas = sh.worksheet("notas")
        raw_notas = ws_notas.get_all_records()

        if not raw_notas:
            st.error("⚠️ La hoja 'notas' está vacía. Use el botón de Admin para sincronizar.")
            return df_al, df_cu, pd.DataFrame(), df_in

        df_no = pd.DataFrame(raw_notas)
        df_no.columns = [str(c).strip().upper() for c in df_no.columns]
        df_no['ROW_INDEX'] = [i + 2 for i in range(len(raw_notas))]

        # Validar columnas críticas
        if 'ID_CURSO' not in df_no.columns:
            st.error(f"❌ No se encontró 'ID_CURSO' en 'notas'. Columnas: {df_no.columns.tolist()}")
            return df_al, df_cu, pd.DataFrame(), df_in

        all_cols = [c for c in df_no.columns.tolist() if c != 'ROW_INDEX']

        # Detectar columnas de notas (entre ID_CURSO y COMENTARIOS)
        try:
            idx_start = all_cols.index('ID_CURSO') + 1
            idx_end = next(i for i, c in enumerate(all_cols) if "COMENTARIO" in c)
            notas_cols = all_cols[idx_start:idx_end]
        except:
            notas_cols = all_cols[2:-1] if len(all_cols) > 3 else []

        for c in notas_cols:
            df_no[c] = pd.to_numeric(df_no[c], errors='coerce').fillna(0)

        st.session_state['notas_header_list'] = notas_cols
        st.session_state['full_header_list'] = all_cols

        return df_al, df_cu, df_no, df_in
    except Exception as e:
        st.error(f"❌ Error en carga: {e}")
        return None, None, None, None


def sincronizar_matriz_notas(df_alumnos, df_cursos, df_notas_actuales):
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        ws_notas = gc.open_by_url(st.secrets["cursos_sheet_url"]).worksheet("notas")

        # Normalizar datos para comparar
        alumnos_dni = [str(d).strip() for d in df_alumnos['DNI'].unique()]
        cursos_id = [str(c).strip().upper() for c in df_cursos['D_CURSO'].unique()]

        existentes = set()
        if not df_notas_actuales.empty:
            existentes = set(zip(df_notas_actuales['DNI'].astype(str).str.strip(),
                                 df_notas_actuales['ID_CURSO'].astype(str).str.strip().upper()))

        nuevas_filas = []
        for c in cursos_id:
            for a in alumnos_dni:
                if (a, c) not in existentes:
                    # [DNI, ID_CURSO, 0, 0... , ""]
                    fila = [a, c] + [0] * len(st.session_state.get('notas_header_list', [])) + [""]
                    nuevas_filas.append(fila)

        if nuevas_filas:
            ws_notas.append_rows(nuevas_filas)
            st.success(f"✅ Sincronizado: {len(nuevas_filas)} filas nuevas creadas.")
            st.cache_data.clear()
            st.rerun()
    except Exception as e:
        st.error(f"❌ Error sincronizando: {e}")


def guardar_cambios(edited_df):
    if not isinstance(edited_df, pd.DataFrame) or edited_df.empty: return
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        ws = gc.open_by_url(st.secrets["cursos_sheet_url"]).worksheet("notas")
        headers = st.session_state['full_header_list']

        batch = []
        col_coment = [c for c in edited_df.columns if "COMENTARIO" in c][0]
        columns_to_save = st.session_state['notas_header_list'] + [col_coment]

        for i in range(len(edited_df)):
            fila = edited_df.iloc[i]
            gs_row = int(fila['ROW_INDEX'])
            for col in columns_to_save:
                if col in fila:
                    val = fila[col]
                    if hasattr(val, "item"): val = val.item()  # Convertir int64 a int nativo
                    if pd.isna(val): val = ""
                    col_gs = headers.index(col) + 1
                    batch.append({'range': utils.rowcol_to_a1(gs_row, col_gs), 'values': [[val]]})

        if batch:
            ws.batch_update(batch)
            st.success("✅ Datos guardados.")
            st.cache_data.clear()
            st.rerun()
    except Exception as e:
        st.error(f"❌ Error al guardar: {e}")


def procesar_datos(df_al, df_cu, df_no):
    if df_no is None or df_no.empty: return pd.DataFrame()

    # Cruce seguro (todo normalizado a mayúsculas)
    df_no['DNI'] = df_no['DNI'].astype(str).str.strip()
    df_al['DNI'] = df_al['DNI'].astype(str).str.strip()
    df_no['ID_CURSO'] = df_no['ID_CURSO'].astype(str).str.strip().upper()
    df_cu['D_CURSO'] = df_cu['D_CURSO'].astype(str).str.strip().upper()

    df = pd.merge(df_no, df_al[['DNI', 'NOMBRE', 'APELLIDO']], on='DNI', how='left')
    df = pd.merge(df, df_cu[['D_CURSO', 'ASIGNATURA']], left_on='ID_CURSO', right_on='D_CURSO', how='left')

    def extraer_principal(val):
        res = re.match(r'^([A-Z0-9]+)', str(val))
        return res.group(1) if res else "OTROS"

    df[CURSO_PRINCIPAL] = df['ID_CURSO'].apply(extraer_principal)
    return df


# --- INTERFAZ ---
st.set_page_config(page_title="Sistema de Notas Pro", layout="wide")
df_al, df_cu, df_no, df_in = load_data_online()

if df_no is not None and not df_no.empty:
    df_final = procesar_datos(df_al, df_cu, df_no)
else:
    df_final = pd.DataFrame()

if 'logeado' not in st.session_state: st.session_state.logeado = False

if not st.session_state.logeado:
    st.title("🔐 Acceso Docente")
    dni_in = st.text_input("DNI")
    pass_in = st.text_input("Contraseña", type="password")
    if st.button("Entrar"):
        if df_in is not None and not df_in.empty:
            verif = df_in[(df_in['DNI_DOCENTE'].astype(str) == dni_in) & (df_in['CLAVE_ACCESO'].astype(str) == pass_in)]
            if not verif.empty:
                st.session_state.logeado = True
                st.session_state.dni = dni_in
                st.session_state.cursos = [str(c).upper().strip() for c in verif['ID_CURSO'].tolist()]
                st.rerun()
            else:
                st.error("DNI o Clave incorrectos.")
else:
    with st.sidebar:
        st.write(f"Docente: {st.session_state.dni}")
        if str(st.session_state.dni) == DNI_ADMIN:
            st.markdown("---")
            st.subheader("🛠 Panel de Administración")
            if st.button("🔄 Sincronizar Alumnos"):
                sincronizar_matriz_notas(df_al, df_cu, df_no)
        if st.button("Cerrar Sesión"):
            st.session_state.logeado = False
            st.rerun()

    if df_final.empty:
        st.warning("⚠️ No hay datos de alumnos en la hoja de notas. Si es administrador, use 'Sincronizar'.")
    else:
        mis_cursos = st.session_state.cursos
        df_mio = df_final[df_final['ID_CURSO'].isin(mis_cursos)]
        grupos = sorted(df_mio[CURSO_PRINCIPAL].unique())

        if len(grupos) > 0:
            tabs = st.tabs(list(grupos))
            cols_n = st.session_state.get('notas_header_list', [])
            for i, g in enumerate(grupos):
                with tabs[i]:
                    df_tab = df_mio[df_mio[CURSO_PRINCIPAL] == g].reset_index(drop=True)
                    # Detectar columna de comentario de forma flexible
                    col_coment = [c for c in df_tab.columns if "COMENTARIO" in c][0]
                    cols_show = ['NOMBRE', 'APELLIDO', 'ID_CURSO'] + cols_n + [col_coment]
                    cols_edit = ['ROW_INDEX'] + cols_show

                    config = {
                        "ROW_INDEX": st.column_config.Column(disabled=True, width="small"),
                        "NOMBRE": st.column_config.Column(disabled=True),
                        "APELLIDO": st.column_config.Column(disabled=True),
                        "ID_CURSO": st.column_config.Column(disabled=True)
                    }

                    edited_df = st.data_editor(df_tab[cols_edit], column_config=config, key=f"ed_{g}",
                                               use_container_width=True, hide_index=True)
                    if st.button(f"Guardar Cambios {g}", key=f"btn_{g}"):
                        guardar_cambios(edited_df)
        else:
            st.warning("No tiene materias asignadas o no hay alumnos sincronizados en ellas.")