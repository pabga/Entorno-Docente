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

        # Carga cruda de pestañas
        df_al = pd.DataFrame(sh.worksheet("alumnos").get_all_records())
        df_cu = pd.DataFrame(sh.worksheet("cursos").get_all_records())
        df_no = pd.DataFrame(sh.worksheet("notas").get_all_records())
        df_in = pd.DataFrame(sh.worksheet("instructores").get_all_records())

        # --- DETECCIÓN AUTOMÁTICA DE COLUMNAS DE NOTAS ---
        # Tomamos todos los nombres de columnas de la hoja notas
        all_cols = df_no.columns.tolist()

        # Lógica: Las notas son todo lo que esté DESPUÉS de ID_CURSO (índice 1)
        # y ANTES de Comentarios_Docente (última columna)
        if len(all_cols) > 3:
            # Buscamos dinámicamente dónde termina ID_CURSO
            idx_inicio = all_cols.index(ID_CURSO_NOTAS) + 1
            # Buscamos la columna de comentarios (o usamos la última si no existe)
            try:
                idx_fin = all_cols.index('Comentarios_Docente')
            except:
                idx_fin = len(all_cols) - 1

            notas_cols = all_cols[idx_inicio:idx_fin]
        else:
            notas_cols = []

        # Convertir notas a números (importante para el editor)
        for c in notas_cols:
            df_no[c] = pd.to_numeric(df_no[c], errors='coerce').fillna(0)

        st.session_state['notas_header_list'] = notas_cols
        st.session_state['full_header_list'] = all_cols

        return df_al, df_cu, df_no, df_in

    except Exception as e:
        st.error(f"Error técnico al conectar con Drive: {e}")
        return None, None, None, None


# ----------------------------------------------------------------------
#                         PROCESAMIENTO LÓGICO
# ----------------------------------------------------------------------

def procesar_datos(df_al, df_cu, df_no):
    if df_al is None or df_no is None: return pd.DataFrame()

    # Normalización de IDs para el cruce (Merge)
    df_no[ID_ALUMNO] = df_no[ID_ALUMNO].astype(str).str.strip()
    df_al[ID_ALUMNO] = df_al[ID_ALUMNO].astype(str).str.strip()
    df_no[ID_CURSO_NOTAS] = df_no[ID_CURSO_NOTAS].astype(str).str.strip().str.upper()
    df_cu[ID_CURSO_MAESTRO] = df_cu[ID_CURSO_MAESTRO].astype(str).str.strip().str.upper()

    # Cruce 1: Notas + Alumnos (trae Nombre/Apellido)
    df = pd.merge(df_no, df_al[[ID_ALUMNO, 'Nombre', 'Apellido']], on=ID_ALUMNO, how='left')

    # Cruce 2: Notas + Cursos (trae nombre de Asignatura)
    df = pd.merge(df, df_cu[[ID_CURSO_MAESTRO, 'Asignatura']], left_on=ID_CURSO_NOTAS, right_on=ID_CURSO_MAESTRO,
                  how='left')

    # Identificar Curso Principal (Primeras letras antes del guion)
    def extraer_principal(val):
        res = re.match(r'^([A-Z0-9]+)', str(val))
        return res.group(1) if res else "OTROS"

    df[CURSO_PRINCIPAL] = df[ID_CURSO_NOTAS].apply(extraer_principal)

    return df


# ----------------------------------------------------------------------
#                         GUARDADO
# ----------------------------------------------------------------------

def guardar_cambios(edit_dict):
    if not edit_dict['edited_rows']: return
    try:
        # Re-auth para escritura
        gcp_dict = {
            k: st.secrets[f"gcp_service_account_{k}"] if f"gcp_service_account_{k}" in st.secrets else st.secrets[k] for
            k in ["type", "project_id", "private_key_id", "private_key", "client_email", "client_id", "auth_uri",
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
            st.success("✅ Datos sincronizados con Google Drive.")
            st.cache_data.clear()
            st.rerun()
    except Exception as e:
        st.error(f"Error al guardar: {e}")


# ----------------------------------------------------------------------
#                         FLUJO DE LA APP
# ----------------------------------------------------------------------

st.set_page_config(page_title="Sistema de Notas", layout="wide")

df_al, df_cu, df_no, df_in = load_data_online()
df_final = procesar_datos(df_al, df_cu, df_no)

if 'logeado' not in st.session_state: st.session_state.logeado = False

if not st.session_state.logeado:
    st.title("🚀 Acceso Docente")
    user = st.text_input("DNI Docente")
    passw = st.text_input("Contraseña", type="password")
    if st.button("Iniciar Sesión"):
        verif = df_in[(df_in['DNI_DOCENTE'].astype(str) == user) & (df_in['Clave_Acceso'].astype(str) == passw)]
        if not verif.empty:
            st.session_state.logeado = True
            st.session_state.dni = user
            st.session_state.cursos = verif['ID_CURSO'].tolist()
            st.rerun()
        else:
            st.error("DNI o Clave incorrectos.")
else:
    # Dashboard logeado
    st.sidebar.title(f"Usuario: {st.session_state.dni}")
    if st.sidebar.button("Cerrar Sesión"):
        st.session_state.logeado = False
        st.rerun()

    # Filtrar solo lo que le pertenece a este docente
    mis_cursos = [str(c).strip().upper() for c in st.session_state.cursos]
    df_mio = df_final[df_final[ID_CURSO_NOTAS].isin(mis_cursos)]

    # Crear Tabs por Curso Principal (PCA, PPH, etc.)
    grupos = df_mio[CURSO_PRINCIPAL].unique()

    if len(grupos) > 0:
        tabs = st.tabs(list(grupos))
        cols_n = st.session_state['notas_header_list']

        for i, g in enumerate(grupos):
            with tabs[i]:
                df_tab = df_mio[df_mio[CURSO_PRINCIPAL] == g].reset_index(drop=True)

                st.subheader(f"📋 Registro de Notas: {g}")

                # Definir columnas a mostrar en el editor
                ver = ['Nombre', 'Apellido', 'Asignatura', ID_CURSO_NOTAS] + cols_n + ['Comentarios_Docente']

                # Bloquear columnas que no son notas para que no se puedan editar
                bloqueo = {c: st.column_config.Column(disabled=True) for c in
                           ['Nombre', 'Apellido', 'Asignatura', ID_CURSO_NOTAS]}

                # EL EDITOR DE DATOS
                edicion = st.data_editor(df_tab[ver], column_config=bloqueo, key=f"editor_{g}",
                                         use_container_width=True)

                if st.button(f"💾 Guardar Cambios {g}", key=f"btn_{g}"):
                    guardar_cambios(st.session_state[f"editor_{g}"])
    else:
        st.warning(
            "No se encontraron registros de notas para tus cursos asignados. Verifica que los códigos en la pestaña 'notas' coincidan con los de 'instructores'.")