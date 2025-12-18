import streamlit as st
import pandas as pd
import gspread
import numpy as np
import re
from gspread import utils

# --- CONFIGURACIÓN DE IDENTIFICADORES ---
ID_ALUMNO = 'DNI'
ID_CURSO_NOTAS = 'ID_CURSO'
ID_CURSO_MAESTRO = 'D_CURSO'
CURSO_PRINCIPAL = 'CURSO_PRINCIPAL'
# --- DEFINE AQUÍ TU DNI DE ADMINISTRADOR ---
DNI_ADMIN = "41209872"


# ----------------------------------------------------------------------
#                         CONEXIÓN Y CARGA DE DATOS
# ----------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_data_online():
    try:
        gcp_dict = {
            "type": st.secrets["gcp_service_account_type"],
            "project_id": st.secrets["gcp_service_account_project_id"],
            "private_key_id": st.secrets["gcp_service_account_private_key_id"],
            "private_key": st.secrets["gcp_service_account_private_key"],
            "client_email": st.secrets["gcp_service_account_client_email"],
            "client_id": st.secrets["gcp_service_account_client_id"],
            "auth_uri": st.secrets["gcp_service_account_auth_uri"],
            "token_uri": st.secrets["gcp_service_account_token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_service_account_auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_service_account_client_x509_cert_url"]
        }

        gc = gspread.service_account_from_dict(gcp_dict)
        sh = gc.open_by_url(st.secrets["cursos_sheet_url"])

        df_al = pd.DataFrame(sh.worksheet("alumnos").get_all_records())
        df_cu = pd.DataFrame(sh.worksheet("cursos").get_all_records())
        df_no = pd.DataFrame(sh.worksheet("notas").get_all_records())
        df_in = pd.DataFrame(sh.worksheet("instructores").get_all_records())

        all_cols = df_no.columns.tolist()

        # DETECCIÓN DINÁMICA: Notas entre ID_CURSO y Comentarios_Docente
        try:
            idx_inicio = all_cols.index(ID_CURSO_NOTAS) + 1
            idx_fin = all_cols.index('Comentarios_Docente')
            notas_cols = all_cols[idx_inicio:idx_fin]
        except:
            # Si algo falla, asume que desde la 3ra columna hasta la penúltima son notas
            notas_cols = all_cols[2:-1] if len(all_cols) > 3 else []

        for c in notas_cols:
            df_no[c] = pd.to_numeric(df_no[c], errors='coerce').fillna(0)

        st.session_state['notas_header_list'] = notas_cols
        st.session_state['full_header_list'] = all_cols

        return df_al, df_cu, df_no, df_in

    except Exception as e:
        st.error(f"Error técnico al cargar: {e}")
        return None, None, None, None


# ----------------------------------------------------------------------
#                         ACCIONES DE ADMIN (COLUMNAS)
# ----------------------------------------------------------------------

def add_col_admin(new_name):
    try:
        gcp_dict = {k: st.secrets[k] for k in
                    ["type", "project_id", "private_key_id", "private_key", "client_email", "client_id", "auth_uri",
                     "token_uri", "auth_provider_x509_cert_url", "client_x509_cert_url"]}
        gc = gspread.service_account_from_dict(gcp_dict)
        ws = gc.open_by_url(st.secrets["cursos_sheet_url"]).worksheet("notas")

        headers = st.session_state['full_header_list']
        # Insertar antes de Comentarios_Docente
        try:
            pos = headers.index('Comentarios_Docente') + 1
        except:
            pos = len(headers) + 1

        ws.insert_cols([[]], col=pos)
        ws.update_cell(1, pos, new_name)

        st.success(f"Columna '{new_name}' agregada.")
        st.cache_data.clear()
        st.rerun()
    except Exception as e:
        st.error(f"Error al crear columna: {e}")


# ----------------------------------------------------------------------
#                         LÓGICA Y GUARDADO
# ----------------------------------------------------------------------

def procesar_datos(df_al, df_cu, df_no):
    if df_al is None or df_no is None: return pd.DataFrame()
    df_no[ID_ALUMNO] = df_no[ID_ALUMNO].astype(str).str.strip()
    df_al[ID_ALUMNO] = df_al[ID_ALUMNO].astype(str).str.strip()
    df_no[ID_CURSO_NOTAS] = df_no[ID_CURSO_NOTAS].astype(str).str.strip().upper()
    df_cu[ID_CURSO_MAESTRO] = df_cu[ID_CURSO_MAESTRO].astype(str).str.strip().upper()

    df = pd.merge(df_no, df_al[[ID_ALUMNO, 'Nombre', 'Apellido']], on=ID_ALUMNO, how='left')
    df = pd.merge(df, df_cu[[ID_CURSO_MAESTRO, 'Asignatura']], left_on=ID_CURSO_NOTAS, right_on=ID_CURSO_MAESTRO,
                  how='left')

    def extraer_principal(val):
        res = re.match(r'^([A-Z0-9]+)', str(val))
        return res.group(1) if res else "OTROS"

    df[CURSO_PRINCIPAL] = df[ID_CURSO_NOTAS].apply(extraer_principal)
    return df


def guardar_cambios(edit_dict):
    if not edit_dict['edited_rows']: return
    try:
        gcp_dict = {k: st.secrets[k] for k in
                    ["type", "project_id", "private_key_id", "private_key", "client_email", "client_id", "auth_uri",
                     "token_uri", "auth_provider_x509_cert_url", "client_x509_cert_url"]}
        gc = gspread.service_account_from_dict(gcp_dict)
        ws = gc.open_by_url(st.secrets["cursos_sheet_url"]).worksheet("notas")
        headers = st.session_state['full_header_list']
        batch = []
        for row_idx, cambios in edit_dict['edited_rows'].items():
            fila_gs = int(row_idx) + 2
            for col_nombre, nuevo_valor in cambios.items():
                col_gs = headers.index(col_nombre) + 1
                batch.append({'range': utils.rowcol_to_a1(fila_gs, col_gs), 'values': [[nuevo_valor]]})
        if batch:
            ws.batch_update(batch)
            st.success("✅ Cambios guardados.")
            st.cache_data.clear()
            st.rerun()
    except Exception as e:
        st.error(f"Error al guardar: {e}")


# ----------------------------------------------------------------------
#                         INTERFAZ
# ----------------------------------------------------------------------

st.set_page_config(page_title="Gestión de Notas", layout="wide")

df_al, df_cu, df_no, df_in = load_data_online()
df_final = procesar_datos(df_al, df_cu, df_no)

if 'logeado' not in st.session_state: st.session_state.logeado = False

if not st.session_state.logeado:
    st.title("🔐 Acceso al Sistema")
    dni_in = st.text_input("DNI Docente")
    pass_in = st.text_input("Clave", type="password")
    if st.button("Entrar"):
        verif = df_in[(df_in['DNI_DOCENTE'].astype(str) == dni_in) & (df_in['Clave_Acceso'].astype(str) == pass_in)]
        if not verif.empty:
            st.session_state.logeado = True
            st.session_state.dni = dni_in
            st.session_state.cursos = verif['ID_CURSO'].tolist()
            st.rerun()
        else:
            st.error("Datos incorrectos.")
else:
    # Sidebar: Salir y Herramientas Admin
    st.sidebar.title(f"Usuario: {st.session_state.dni}")
    if st.sidebar.button("Cerrar Sesión"):
        st.session_state.logeado = False
        st.rerun()

    # SOLO SI ES ADMIN APARECE EL BOTÓN DE AGREGAR EXAMEN
    if st.session_state.dni == DNI_ADMIN:
        st.sidebar.markdown("---")
        st.sidebar.subheader("🛠 Panel de Administrador")
        nuevo_ex = st.sidebar.text_input("Nombre de nueva evaluación")
        if st.sidebar.button("Añadir Columna"):
            if nuevo_ex: add_col_admin(nuevo_ex)

    # Dashboard de Tabs
    mis_cursos = [str(c).strip().upper() for c in st.session_state.cursos]
    df_mio = df_final[df_final[ID_CURSO_NOTAS].isin(mis_cursos)]
    grupos = df_mio[CURSO_PRINCIPAL].unique()

    if len(grupos) > 0:
        tabs = st.tabs(list(grupos))
        cols_n = st.session_state['notas_header_list']
        for i, g in enumerate(grupos):
            with tabs[i]:
                df_tab = df_mio[df_mio[CURSO_PRINCIPAL] == g].reset_index(drop=True)
                st.subheader(f"Materia: {g}")
                ver = ['Nombre', 'Apellido', ID_CURSO_NOTAS] + cols_n + ['Comentarios_Docente']
                bloqueo = {c: st.column_config.Column(disabled=True) for c in ['Nombre', 'Apellido', ID_CURSO_NOTAS]}

                edicion = st.data_editor(df_tab[ver], column_config=bloqueo, key=f"ed_{g}", use_container_width=True)
                if st.button(f"Guardar {g}", key=f"btn_{g}"):
                    guardar_changes(st.session_state[f"ed_{g}"])
    else:
        st.warning("Sin cursos asignados.")