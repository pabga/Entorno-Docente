import streamlit as st
import pandas as pd
import gspread
import numpy as np
import json
from gspread import utils

# --- CONFIGURACIÓN DE COLUMNAS Y DATOS MAESTROS ---
COLUMNAS_NOTAS = ['Parcial 1', 'Parcial 2']
ID_ALUMNO = 'DNI'
ID_CURSO_NOTAS = 'ID_CURSO'
ID_CURSO_MAESTRO = 'D_CURSO'

# Base de datos de docentes con su DNI y sus cursos asignados (Simulación/Lectura)
DOCENTES_ASIGNADOS = {
    '17999767': ['MMA-FH-2025-P', 'MMA-FH-2025-F'],  # Graciela
    '27642905': ['AER-MAT-2025-P'],  # Rodolfo
    '22041283': ['MMA-FH-2025-P', 'MMA-FH-2025-F']  # Daniel
}


# ----------------------------------------------------------------------
#                         CONEXIÓN Y CARGA DE DATOS REALES (GSPREAD)
# ----------------------------------------------------------------------

@st.cache_data(ttl=600)  # Cacha los datos por 10 minutos
def load_data_online():
    """
    Carga los DataFrames de Google Sheets reconstruyendo el diccionario de credenciales
    a partir de las claves planas de st.secrets.
    """
    try:
        # 1. RECONSTRUCCIÓN DEL DICCIONARIO DE CREDENCIALES (a partir de CLAVES PLANAS)
        # Este método evita el error de KeyError al buscar 'gcp_credentials_json'
        gcp_service_account_dict = {
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
            # Usamos .get() por si 'universe_domain' no está en el JSON original
            "universe_domain": st.secrets.get("gcp_service_account_universe_domain", "googleapis.com")
        }

        # 2. AUTENTICACIÓN
        gc = gspread.service_account_from_dict(gcp_service_account_dict)

        # 3. ACCESO A URLs (que son la misma URL del archivo central)
        url_archivo_central = st.secrets["cursos_sheet_url"]

        # 4. LECTURA DE DATOS de las tres pestañas
        archivo_sheets = gc.open_by_url(url_archivo_central)
        df_alumnos = pd.DataFrame(archivo_sheets.worksheet("alumnos").get_all_records())
        df_cursos = pd.DataFrame(archivo_sheets.worksheet("cursos").get_all_records())
        df_notas_brutas = pd.DataFrame(archivo_sheets.worksheet("notas").get_all_records())

        # Limpieza: Asegurar que las columnas de notas sean numéricas y sin NaN
        cols_para_limpiar = COLUMNAS_NOTAS
        df_notas_brutas[cols_para_limpiar] = df_notas_brutas[cols_para_limpiar].apply(pd.to_numeric,
                                                                                      errors='coerce').fillna(0)

        # Guardar las columnas originales de df_notas_brutas para el mapeo de guardado
        st.session_state['notas_columns'] = df_notas_brutas.columns.tolist()

        return df_alumnos, df_cursos, df_notas_brutas

    except KeyError as k_e:
        st.error(
            f"Error de configuración. Clave faltante en Streamlit Secrets: {k_e}. Revise la tipografía de todas las claves en la nube.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    except gspread.exceptions.APIError as api_e:
        st.error(
            f"Error de API de Google (Permisos/Conexión). Asegúrese de que el correo de la Cuenta de Servicio tiene acceso de Editor al archivo. Detalle: {api_e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        st.error(f"Error general al cargar los datos. Detalle: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# ----------------------------------------------------------------------
#                         FUNCIÓN DE CÁLCULO Y INTEGRACIÓN
# ----------------------------------------------------------------------

def integrar_y_calcular(df_alumnos, df_cursos, df_notas):
    """
    Integra los datos de Alumnos, Cursos y Notas, y calcula el promedio por materia.
    """

    if df_alumnos.empty or df_cursos.empty or df_notas.empty:
        return pd.DataFrame()

    # 1. Unir Notas con Alumnos (Usando DNI)
    df_paso1 = pd.merge(
        df_notas,
        df_alumnos[[ID_ALUMNO, 'Nombre', 'Apellido']],
        on=ID_ALUMNO,
        how='left'
    )

    # 2. Unir con Cursos (CORREGIDO: Cruza ID_CURSO_NOTAS con ID_CURSO_MAESTRO)
    df_final = pd.merge(
        df_paso1,
        df_cursos[[ID_CURSO_MAESTRO, 'Asignatura']],
        left_on=ID_CURSO_NOTAS,
        right_on=ID_CURSO_MAESTRO,
        how='left'
    )

    # Eliminamos la columna duplicada de la clave de cruce
    df_final.drop(columns=[ID_CURSO_MAESTRO], inplace=True)

    # 3. CÁLCULO DE PROMEDIOS
    df_final['Promedio_Materia'] = df_final[COLUMNAS_NOTAS].mean(axis=1).round(2)

    return df_final


# ----------------------------------------------------------------------
#                         FUNCIÓN DE GUARDADO PERSISTENTE (GSPREAD)
# ----------------------------------------------------------------------

def save_data_to_gsheet(df_original_notas_base, edited_data):
    """
    Identifica los cambios en el editor y los escribe en la Hoja de Google de Notas.
    """
    if not edited_data['edited_rows']:
        st.warning("No se detectaron cambios para guardar.")
        return

    try:
        # Reconstrucción de credenciales para el guardado (igual que en load_data_online)
        gcp_service_account_dict = {
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

        gc = gspread.service_account_from_dict(gcp_service_account_dict)
        url_archivo_central = st.secrets["cursos_sheet_url"]

        spreadsheet = gc.open_by_url(url_archivo_central)
        worksheet = spreadsheet.worksheet("notas")

        updates = []
        original_cols = st.session_state['notas_columns']

        for df_index, col_updates in edited_data['edited_rows'].items():

            gsheet_row_index = df_index + 2  # Fila 1 es encabezado, fila 2 es índice 0

            for col_name, new_value in col_updates.items():

                if col_name not in original_cols:
                    continue

                col_index = original_cols.index(col_name) + 1

                cell_a1 = utils.rowcol_to_a1(gsheet_row_index, col_index)

                updates.append({
                    'range': cell_a1,
                    'values': [[new_value]]
                })

        if updates:
            worksheet.batch_update(updates)
            st.success(f"💾 ¡{len(updates)} cambios guardados exitosamente en Google Sheets!")

            st.cache_data.clear()
            st.rerun()

    except Exception as e:
        st.error(f"Error crítico al guardar. Revise permisos de Editor en la hoja de notas. Detalle: {e}")


# ----------------------------------------------------------------------
#                         EJECUCIÓN PRINCIPAL Y STREAMLIT
# ----------------------------------------------------------------------

st.set_page_config(page_title="Dashboard de Notas Docente (Online)", layout="wide")

# 1. CARGA Y CÁLCULO INICIAL (Se ejecuta y guarda en session_state)
if 'df_final_completo' not in st.session_state:
    with st.spinner("Cargando y validando datos desde Google Drive..."):
        df_alumnos_full, df_cursos_full, df_notas_brutas_full = load_data_online()

    if df_alumnos_full.empty or df_cursos_full.empty or df_notas_brutas_full.empty:
        st.stop()

    df_final_full = integrar_y_calcular(df_alumnos_full, df_cursos_full, df_notas_brutas_full)

    # --- ¡SECCIÓN CORREGIDA AÑADIDA AQUÍ! ---
    # Guardamos los encabezados de notas en la sesión si aún no existen
    if 'notas_columns' not in st.session_state and not df_notas_brutas_full.empty:
        st.session_state['notas_columns'] = df_notas_brutas_full.columns.tolist()
    # ------------------------------------------

    st.session_state['df_final_completo'] = df_final_full
    st.session_state['df_notas_base'] = df_notas_brutas_full

# Inicializar el estado de sesión (autenticación)
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
    st.session_state['docente_dni'] = None


# --- Función de Login ---
def login_form():
    st.sidebar.header("Inicio de Sesión")

    default_password = st.secrets.get("app_password", "1234")

    with st.sidebar.form("login_form"):
        dni_input = st.text_input("DNI del Docente (Ej: 17999767)", value="")
        password_input = st.text_input("Contraseña", type="password")
        submitted = st.form_submit_button("Ingresar")

        if submitted:
            if dni_input in DOCENTES_ASIGNADOS and password_input == default_password:
                st.session_state['authenticated'] = True
                st.session_state['docente_dni'] = dni_input
                st.sidebar.success(f"Bienvenido Docente con DNI: {dni_input}")
                st.rerun()
            else:
                st.sidebar.error("DNI o Contraseña incorrectos.")


# --- Función principal de Visualización y Edición ---
def show_dashboard_filtrado(docente_dni):
    cursos_asignados = DOCENTES_ASIGNADOS.get(docente_dni, [])

    if not cursos_asignados:
        st.warning("Usted no tiene cursos asignados en el sistema.")
        return

    st.title(f'👩‍🏫 Dashboard Docente - DNI: {docente_dni}')
    st.info(
        f"Conectado a Google Sheets. Mostrando datos solo para sus cursos asignados: **{', '.join(cursos_asignados)}**")
    st.markdown('***')

    # 1. FILTRADO DEL DATAFRAME BASE COMPLETO
    df_final_completo = st.session_state['df_final_completo']
    df_filtrado_docente_base = df_final_completo[
        df_final_completo[ID_CURSO_NOTAS].isin(cursos_asignados)
    ].reset_index(drop=True).copy()

    if 'Comentarios_Docente' not in df_filtrado_docente_base.columns:
        df_filtrado_docente_base['Comentarios_Docente'] = ''

    # 2. INTERFAZ DE EDICIÓN CON BOTÓN DE GUARDAR

    columnas_visibles_editor = ['Nombre', 'Apellido', 'Asignatura', ID_CURSO_NOTAS] + COLUMNAS_NOTAS + [
        'Comentarios_Docente']

    st.header('📝 Edición de Notas y Comentarios')
    st.warning(
        "🚨 **Edite directamente las notas y comentarios. Use el botón 'Guardar Cambios' para persistir la data en Drive.**")

    with st.form("notas_form"):

        df_editado_with_info = st.data_editor(
            df_filtrado_docente_base[columnas_visibles_editor],
            column_config={
                "Parcial 1": st.column_config.NumberColumn("Parcial 1", min_value=0.0, max_value=10.0, format="%.1f"),
                "Parcial 2": st.column_config.NumberColumn("Parcial 2", min_value=0.0, max_value=10.0, format="%.1f"),
                "Comentarios_Docente": st.column_config.TextColumn("Descripción/Comentario (Editable)"),
                "Nombre": st.column_config.TextColumn("Nombre", disabled=True),
                "Apellido": st.column_config.TextColumn("Apellido", disabled=True),
                "Asignatura": st.column_config.TextColumn("Asignatura", disabled=True),
                ID_CURSO_NOTAS: st.column_config.TextColumn("ID_CURSO", disabled=True),
            },
            hide_index=False,
            use_container_width=True,
            key="editor_notas"
        )

        save_button = st.form_submit_button("💾 Guardar Cambios en Google Sheets")

        if save_button:
            edited_data = st.session_state["editor_notas"]
            save_data_to_gsheet(st.session_state['df_notas_base'], edited_data)

    st.markdown('***')

    # 3. RECALCULO DE PROMEDIOS CON LOS DATOS EDITADOS
    df_notas_editadas = df_editado_with_info[['Nombre', 'Apellido'] + COLUMNAS_NOTAS].copy()

    df_notas_editadas['Promedio_Materia'] = df_notas_editadas[COLUMNAS_NOTAS].mean(axis=1).round(2)

    df_promedio_docente = df_notas_editadas.groupby(['Nombre', 'Apellido'])['Promedio_Materia'].mean().reset_index()
    df_promedio_docente.rename(columns={'Promedio_Materia': 'Promedio General de sus Cursos'}, inplace=True)
    df_promedio_docente = df_promedio_docente.sort_values(by='Promedio General de sus Cursos', ascending=False).round(2)

    # 4. VISTA DE RESULTADOS ACTUALIZADOS
    st.header('📈 Promedios Actualizados (Post-Edición)')
    st.dataframe(df_promedio_docente, use_container_width=True)


# --- EJECUCIÓN PRINCIPAL FINAL ---
if st.session_state['authenticated']:
    show_dashboard_filtrado(st.session_state['docente_dni'])

    if st.sidebar.button("Cerrar Sesión"):
        st.session_state['authenticated'] = False
        st.session_state['docente_dni'] = None
        st.rerun()
else:
    st.title("Sistema de Notas Escolares")
    st.markdown("Por favor, inicie sesión con su DNI para acceder a sus materias.")
    login_form()