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
DNI_ADMIN = "41209872"


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

        df_al = pd.DataFrame(sh.worksheet("alumnos").get_all_records())
        df_cu = pd.DataFrame(sh.worksheet("cursos").get_all_records())
        # Cargamos notas y agregamos una columna con el índice real de la fila (0-based + 2 para Excel)
        raw_notas = sh.worksheet("notas").get_all_records()
        df_no = pd.DataFrame(raw_notas)
        df_no['row_index'] = [i + 2 for i in range(len(raw_notas))]

        df_no.columns = [c.strip() for c in df_no.columns]
        all_cols = [c for c in df_no.columns.tolist() if c != 'row_index']

        try:
            idx_inicio = all_cols.index(ID_CURSO_NOTAS) + 1
            idx_fin = next(i for i, x in enumerate(all_cols) if "Comentarios" in x)
            notas_cols = all_cols[idx_inicio:idx_fin]
        except:
            notas_cols = all_cols[2:-1] if len(all_cols) > 3 else []

        for c in notas_cols:
            df_no[c] = pd.to_numeric(df_no[c], errors='coerce').fillna(0)

        st.session_state['notas_header_list'] = notas_cols
        st.session_state['full_header_list'] = all_cols

        return df_al, df_cu, df_no, pd.DataFrame(sh.worksheet("instructores").get_all_records())
    except Exception as e:
        st.error(f"Error en carga: {e}")
        return None, None, None, None


def guardar_cambios(edited_df, original_df_with_indices):
    """
    Usa el row_index para guardar exactamente en la fila correcta de Google Sheets.
    """
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        ws = gc.open_by_url(st.secrets["cursos_sheet_url"]).worksheet("notas")
        headers = st.session_state['full_header_list']

        batch = []
        # Iteramos sobre el dataframe que salió del editor
        for i in range(len(edited_df)):
            fila_actual = edited_df.iloc[i]
            # Recuperamos el índice real de la fila en Excel
            gs_row = int(fila_actual['row_index'])

            # Comparamos con el original para ver qué cambió (optimización)
            # Para simplificar, guardamos las columnas de notas y comentarios
            for col in st.session_state['notas_header_list'] + ['Comentarios_Docente']:
                if col in fila_actual:
                    val = fila_actual[col]
                    col_gs = headers.index(col) + 1
                    batch.append({'range': utils.rowcol_to_a1(gs_row, col_gs), 'values': [[val]]})

        if batch:
            ws.batch_update(batch)
            st.success("✅ ¡Cambios en PCA-VA y demás materias guardados correctamente!")
            st.cache_data.clear()
            st.rerun()
    except Exception as e:
        st.error(f"Error al guardar: {e}")


def procesar_datos(df_al, df_cu, df_no):
    if df_al is None or df_no is None: return pd.DataFrame()
    df_no[ID_ALUMNO] = df_no[ID_ALUMNO].astype(str).str.strip()
    df_al[ID_ALUMNO] = df_al[ID_ALUMNO].astype(str).str.strip()
    df_no[ID_CURSO_NOTAS] = df_no[ID_CURSO_NOTAS].astype(str).str.strip().str.upper()
    df_cu[ID_CURSO_MAESTRO] = df_cu[ID_CURSO_MAESTRO].astype(str).str.strip().str.upper()

    df = pd.merge(df_no, df_al[[ID_ALUMNO, 'Nombre', 'Apellido']], on=ID_ALUMNO, how='left')
    df = pd.merge(df, df_cu[[ID_CURSO_MAESTRO, 'Asignatura']], left_on=ID_CURSO_NOTAS, right_on=ID_CURSO_MAESTRO,
                  how='left')

    def extraer_principal(val):
        res = re.match(r'^([A-Z0-9]+)', str(val))
        return res.group(1) if res else "OTROS"

    df[CURSO_PRINCIPAL] = df[ID_CURSO_NOTAS].apply(extraer_principal)
    return df


# --- INTERFAZ ---
st.set_page_config(page_title="Dashboard Docente", layout="wide")
df_al, df_cu, df_no, df_in = load_data_online()
df_final = procesar_datos(df_al, df_cu, df_no)

if 'logeado' not in st.session_state: st.session_state.logeado = False

if not st.session_state.logeado:
    st.title("🔒 Acceso Docente")
    dni_in = st.text_input("DNI")
    pass_in = st.text_input("Contraseña", type="password")
    if st.button("Entrar"):
        verif = df_in[(df_in['DNI_DOCENTE'].astype(str) == dni_in) & (df_in['Clave_Acceso'].astype(str) == pass_in)]
        if not verif.empty:
            st.session_state.logeado = True
            st.session_state.dni = dni_in
            st.session_state.cursos = [str(c).upper().strip() for c in verif['ID_CURSO'].tolist()]
            st.rerun()
        else:
            st.error("Error de acceso.")
else:
    st.sidebar.title(f"DNI: {st.session_state.dni}")
    if st.sidebar.button("Cerrar Sesión"):
        st.session_state.logeado = False
        st.rerun()

    mis_cursos = st.session_state.cursos
    df_mio = df_final[df_final[ID_CURSO_NOTAS].isin(mis_cursos)]
    grupos = sorted(df_mio[CURSO_PRINCIPAL].unique())

    if len(grupos) > 0:
        tabs = st.tabs(list(grupos))
        cols_n = st.session_state.get('notas_header_list', [])
        for i, g in enumerate(grupos):
            with tabs[i]:
                df_tab = df_mio[df_mio[CURSO_PRINCIPAL] == g].reset_index(drop=True)
                st.subheader(f"Materia Principal: {g}")

                # 'row_index' debe estar presente pero lo ocultamos visualmente
                cols_edit = ['row_index', 'Nombre', 'Apellido', ID_CURSO_NOTAS] + cols_n + ['Comentarios_Docente']

                # Ocultamos row_index y bloqueamos lo que no es nota
                config = {
                    "row_index": st.column_config.Column(required=True, width=None, disabled=True, help="ID interno"),
                    "Nombre": st.column_config.Column(disabled=True),
                    "Apellido": st.column_config.Column(disabled=True),
                    ID_CURSO_NOTAS: st.column_config.Column(disabled=True)
                }

                # Usamos el dataframe editado directamente
                edited_df = st.data_editor(df_tab[cols_edit], column_config=config, key=f"ed_{g}",
                                           use_container_width=True, hide_index=True)

                if st.button(f"Guardar Todo en {g}", key=f"btn_{g}"):
                    guardar_cambios(edited_df, df_tab)
    else:
        st.warning("No se encontraron registros.")