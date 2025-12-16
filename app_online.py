import streamlit as st
import pandas as pd
import gspread
import numpy as np
import json
from gspread import utils

# --- CONFIGURACIÓN DE COLUMNAS Y DATOS MAESTROS ---
ID_ALUMNO = 'DNI'
ID_CURSO_NOTAS = 'ID_CURSO'
ID_CURSO_MAESTRO = 'D_CURSO'

DOCENTES_ASIGNADOS = {}
COLUMNAS_NOTAS = []

# --- LISTA MAESTRA DE PESTAÑAS (Fija) ---
# Define los 5 cursos que siempre aparecerán como pestañas.
# ASEGÚRATE DE QUE ESTOS IDs COINCIDAN CON LOS CÓDIGOS LIMPIOS (SIN ESPACIOS, EN MAYÚSCULAS)
CURSOS_PESTANAS_MAESTRO = ['PPHA-IS-2025-P', 'PPHA-IS-2025-F', 'PPHA-ME-2025-P', 'PPHA-ME-2025-F', 'PPHA-NA-2025-P']


# ----------------------------------------------------------------------
#                         CONEXIÓN Y CARGA DE DATOS REALES (GSPREAD)
# ----------------------------------------------------------------------

@st.cache_data(ttl=600)
def load_data_online():
    """
    Carga todos los DataFrames y determina dinámicamente las columnas de notas.
    """
    try:
        # 1. RECONSTRUCCIÓN DEL DICCIONARIO DE CREDENCIALES
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

        # 2. AUTENTICACIÓN
        gc = gspread.service_account_from_dict(gcp_service_account_dict)

        # 3. ACCESO A URLs
        url_archivo_central = st.secrets["cursos_sheet_url"]
        archivo_sheets = gc.open_by_url(url_archivo_central)

        # 4. LECTURA DE DATOS de las pestañas
        df_alumnos = pd.DataFrame(archivo_sheets.worksheet("alumnos").get_all_records())
        df_cursos = pd.DataFrame(archivo_sheets.worksheet("cursos").get_all_records())
        df_notas_brutas = pd.DataFrame(archivo_sheets.worksheet("notas").get_all_records())
        df_instructores = pd.DataFrame(archivo_sheets.worksheet("instructores").get_all_records())

        # 5. IDENTIFICACIÓN DINÁMICA DE COLUMNAS DE NOTAS
        columnas_clave = ['DNI', 'ID_CURSO', 'Comentarios_Docente']

        global COLUMNAS_NOTAS
        COLUMNAS_NOTAS = [col for col in df_notas_brutas.columns.tolist()
                          if col not in columnas_clave]

        # Limpieza de notas (Conversión a numérico y NaN a 0)
        cols_para_limpiar = COLUMNAS_NOTAS
        df_notas_brutas[cols_para_limpiar] = df_notas_brutas[cols_para_limpiar].apply(pd.to_numeric,
                                                                                      errors='coerce').fillna(0)

        st.session_state['notas_columns'] = df_notas_brutas.columns.tolist()

        return df_alumnos, df_cursos, df_notas_brutas, df_instructores

    except KeyError as k_e:
        st.error(
            f"Error de configuración. Clave faltante en Streamlit Secrets: {k_e}. Revise la tipografía de todas las claves en la nube.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    except gspread.exceptions.APIError as api_e:
        st.error(
            f"Error de API de Google (Permisos/Conexión). Asegúrese de que el correo de la Cuenta de Servicio tiene acceso de Editor al archivo. Detalle: {api_e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        st.error(f"Error general al cargar los datos. Detalle: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# ----------------------------------------------------------------------
#                         FUNCIÓN DE CÁLCULO Y INTEGRACIÓN
# ----------------------------------------------------------------------

def integrar_y_calcular(df_alumnos, df_cursos, df_notas):
    """
    Integra los datos de Alumnos, Cursos y Notas, y calcula el promedio por materia.
    """

    if df_alumnos.empty or df_cursos.empty or df_notas.empty:
        return pd.DataFrame()

    # --- CORRECCIÓN DE DNI FALTANTE ---
    df_notas = df_notas[df_notas[ID_ALUMNO].notna()]
    df_notas = df_notas[df_notas[ID_ALUMNO] != '']
    # -----------------------------------

    # 1. Unir Notas con Alumnos (Usando DNI)
    df_paso1 = pd.merge(
        df_notas,
        df_alumnos[[ID_ALUMNO, 'Nombre', 'Apellido']],
        on=ID_ALUMNO,
        how='left'
    )

    # 2. Unir con Cursos (Cruza ID_CURSO_NOTAS con ID_CURSO_MAESTRO)
    df_final = pd.merge(
        df_paso1,
        df_cursos[[ID_CURSO_MAESTRO, 'Asignatura']],
        left_on=ID_CURSO_NOTAS,
        right_on=ID_CURSO_MAESTRO,
        how='left'
    )

    df_final.drop(columns=[ID_CURSO_MAESTRO], inplace=True)

    # 3. CÁLCULO DE PROMEDIOS
    if COLUMNAS_NOTAS:
        df_final['Promedio_Materia'] = df_final[COLUMNAS_NOTAS].mean(axis=1).round(2)
    else:
        df_final['Promedio_Materia'] = 0

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
        # Reconstrucción de credenciales para el guardado
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

            gsheet_row_index = df_index + 2

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

# 1. CARGA Y CÁLCULO INICIAL (Punto de entrada de datos)
if 'df_final_completo' not in st.session_state:
    with st.spinner("Cargando y validando datos desde Google Drive..."):
        df_alumnos_full, df_cursos_full, df_notas_brutas_full, df_instructores_full = load_data_online()

    if df_alumnos_full.empty or df_cursos_full.empty or df_notas_brutas_full.empty or df_instructores_full.empty:
        st.error("No se pudieron cargar todos los datos maestros (incluyendo la lista de instructores).")
        st.stop()

    # --- CONVERSIÓN CRÍTICA DE DNI Y CURSOS A STRING Y LIMPIEZA ---
    try:
        # Función para limpiar columnas de códigos (DNI, ID_CURSO). AGRESIVA.
        def clean_code_column(df, col_name):
            if col_name in df.columns:
                df[col_name] = df[col_name].astype(str)
                df[col_name] = df[col_name].str.replace(r'[.,]', '', regex=True)
                df[col_name] = df[col_name].str.replace(r'\s+', '', regex=True)
            return df


        # Limpieza de DNI en todas las hojas relevantes
        df_instructores_full = clean_code_column(df_instructores_full, 'DNI_DOCENTE')
        df_alumnos_full = clean_code_column(df_alumnos_full, 'DNI')
        df_notas_brutas_full = clean_code_column(df_notas_brutas_full, 'DNI')

        # Limpieza de IDs de Curso en todas las hojas relevantes
        df_instructores_full = clean_code_column(df_instructores_full, 'ID_CURSO')
        df_notas_brutas_full = clean_code_column(df_notas_brutas_full, 'ID_CURSO')
        df_cursos_full = clean_code_column(df_cursos_full, 'D_CURSO')

        # UNIFICAR CÓDIGOS DE CURSO A MAYÚSCULAS (Final Step)
        df_instructores_full['ID_CURSO'] = df_instructores_full['ID_CURSO'].str.upper()
        df_notas_brutas_full['ID_CURSO'] = df_notas_brutas_full['ID_CURSO'].str.upper()
        df_cursos_full['D_CURSO'] = df_cursos_full['D_CURSO'].str.upper()


    except Exception as e:
        st.error(f"Error al intentar limpiar y convertir columnas DNI/Curso a texto: {e}")
        st.stop()
    # ------------------------------------------------------

    # --- PROCESAMIENTO DINÁMICO: Crea los diccionarios de asignaciones y claves ---
    try:
        # 1. Crear el mapa de asignaciones (DNI -> [Cursos])
        docentes_asignados_map = (
            df_instructores_full.groupby('DNI_DOCENTE')['ID_CURSO']
            .apply(list)
            .to_dict()
        )
        st.session_state['docentes_asignados_map'] = docentes_asignados_map

        # 2. Crear el mapa de claves (DNI -> Clave_Acceso)
        docentes_claves_map = (
            df_instructores_full.groupby('DNI_DOCENTE')['Clave_Acceso']
            .first()
            .to_dict()
        )
        st.session_state['docentes_claves_map'] = docentes_claves_map

    except KeyError as k_e:
        st.error(
            f"Error al procesar la hoja 'instructores'. Asegúrese de que existen las columnas 'DNI_DOCENTE', 'ID_CURSO' y 'Clave_Acceso' y que no tienen espacios. Detalle: {k_e}")
        st.stop()

    df_final_full = integrar_y_calcular(df_alumnos_full, df_cursos_full, df_notas_brutas_full)

    if 'notas_columns' not in st.session_state and not df_notas_brutas_full.empty:
        st.session_state['notas_columns'] = df_notas_brutas_full.columns.tolist()

    st.session_state['df_final_completo'] = df_final_full
    st.session_state['df_notas_base'] = df_notas_brutas_full

# Inicializar el estado de sesión (autenticación)
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
    st.session_state['docente_dni'] = None


# --- Función de Login (Validación contra Drive) ---
def login_form():
    st.sidebar.header("Inicio de Sesión")

    docentes_map = st.session_state.get('docentes_asignados_map', {})
    claves_map = st.session_state.get('docentes_claves_map', {})

    with st.sidebar.form("login_form"):
        dni_input = st.text_input("DNI del Docente", value="")
        password_input = st.text_input("Contraseña", type="password")
        submitted = st.form_submit_button("Ingresar")

        if submitted:
            dni_input = str(dni_input).strip()

            if dni_input in docentes_map:
                if password_input == claves_map.get(dni_input):
                    st.session_state['authenticated'] = True
                    st.session_state['docente_dni'] = dni_input
                    st.sidebar.success(f"Bienvenido Docente con DNI: {dni_input}")
                    st.rerun()
                else:
                    st.sidebar.error("Contraseña incorrecta.")
            else:
                st.sidebar.error("DNI no encontrado o no tiene cursos asignados.")


# --- Función principal de Visualización y Edición ---
def show_dashboard_filtrado(docente_dni):
    docentes_map = st.session_state.get('docentes_asignados_map', {})
    cursos_asignados_raw = docentes_map.get(docente_dni, [])

    # Pre-limpieza de la lista de asignados antes de usarla en el filtro
    cursos_asignados = [c.replace('.', '').replace(',', '').replace(' ', '').upper() for c in cursos_asignados_raw if
                        isinstance(c, str)]

    # 1. FILTRADO DEL DATAFRAME BASE COMPLETO
    df_final_completo = st.session_state['df_final_completo']

    # --- DEBUGGING Y RAW DATA ---
    codigos_encontrados_en_notas = df_final_completo[ID_CURSO_NOTAS].unique().tolist()

    st.sidebar.markdown('---')
    st.sidebar.write("### 🚨 DEBUGGING CRÍTICO (RAW DATA)")
    st.sidebar.write("**1. Cursos Asignados (BÚSQUEDA LIMPIA):**")
    st.sidebar.code(repr(cursos_asignados))
    st.sidebar.write("**2. Códigos en Hoja NOTAS (EXISTENCIA LIMPIA):**")
    st.sidebar.code(repr(codigos_encontrados_en_notas))
    st.sidebar.write(f"DNI Logueado: `{docente_dni}`")
    st.sidebar.markdown('---')
    # --- FIN DEBUGGING ---

    st.title(f'👩‍🏫 Dashboard Docente - DNI: {docente_dni}')
    st.info(f"Mostrando el estado de los {len(CURSOS_PESTANAS_MAESTRO)} cursos principales.")
    st.markdown('***')

    # -----------------------------------------------------------
    # INICIO DE ESTRUCTURA DE PESTAÑAS FIJAS
    # -----------------------------------------------------------

    # Creamos un objeto de tabs, usando la lista MAESTRA de cursos
    tabs = st.tabs(CURSOS_PESTANAS_MAESTRO)

    for i, curso_id in enumerate(CURSOS_PESTANAS_MAESTRO):
        with tabs[i]:
            st.header(f"Notas de la Materia: {curso_id}")

            # --- VERIFICACIÓN DE ASIGNACIÓN ---
            if curso_id not in cursos_asignados:
                st.error(f"⛔️ Usted no tiene esta materia asignada ({curso_id}) según la hoja 'instructores'.")
                continue

            # --- 1. FILTRADO: SOLO EL CURSO ACTUAL (Y ASIGNADO) ---
            df_filtrado_curso = df_final_completo[
                df_final_completo[ID_CURSO_NOTAS] == curso_id
                ].reset_index(drop=True).copy()

            if df_filtrado_curso.empty:
                st.warning(f"⚠️ ¡Curso Asignado! Pero no hay notas registradas en la hoja 'notas' para {curso_id}.")
                continue

            # Preparamos las columnas
            if 'Comentarios_Docente' not in df_filtrado_curso.columns:
                df_filtrado_curso['Comentarios_Docente'] = ''

            # 2. INTERFAZ DE EDICIÓN
            columnas_visibles_editor = ['Nombre', 'Apellido', 'Asignatura', ID_CURSO_NOTAS] + COLUMNAS_NOTAS + [
                'Comentarios_Docente']

            st.subheader('📝 Edición de Notas')

            with st.form(f"notas_form_{curso_id}"):

                # Creación dinámica de la configuración de columnas
                column_config_dict = {
                    "Comentarios_Docente": st.column_config.TextColumn("Descripción/Comentario (Editable)"),
                    "Nombre": st.column_config.TextColumn("Nombre", disabled=True),
                    "Apellido": st.column_config.TextColumn("Apellido", disabled=True),
                    "Asignatura": st.column_config.TextColumn("Asignatura", disabled=True),
                    ID_CURSO_NOTAS: st.column_config.TextColumn("ID_CURSO", disabled=True),
                }
                for col in COLUMNAS_NOTAS:
                    column_config_dict[col] = st.column_config.NumberColumn(col, min_value=0.0, max_value=10.0,
                                                                            format="%.1f")

                df_editado_with_info = st.data_editor(
                    df_filtrado_curso[columnas_visibles_editor],
                    column_config=column_config_dict,
                    hide_index=False,
                    use_container_width=True,
                    key=f"editor_notas_{curso_id}"
                )

                save_button = st.form_submit_button(f"💾 Guardar Cambios para {curso_id}")

                if save_button:
                    edited_data = st.session_state[f"editor_notas_{curso_id}"]
                    save_data_to_gsheet(st.session_state['df_notas_base'], edited_data)

            st.markdown('***')

    # -----------------------------------------------------------
    # FIN DE ESTRUCTURA DE PESTAÑAS (TABS)
    # -----------------------------------------------------------

    # 4. RECALCULO Y VISTA DE PROMEDIOS GENERALES (Fuera de los tabs)
    st.header('📈 Promedios Actualizados (General del Docente)')

    df_para_promedio_general = df_final_completo[
        df_final_completo[ID_CURSO_NOTAS].isin(cursos_asignados)
    ].copy()

    if COLUMNAS_NOTAS and not df_para_promedio_general.empty:
        df_para_promedio_general['Promedio_Materia'] = df_para_promedio_general[COLUMNAS_NOTAS].mean(axis=1).round(2)

        df_promedio_docente = df_para_promedio_general.groupby(['Nombre', 'Apellido'])[
            'Promedio_Materia'].mean().reset_index()
        df_promedio_docente.rename(columns={'Promedio_Materia': 'Promedio General de sus Cursos'}, inplace=True)
        df_promedio_docente = df_promedio_docente.sort_values(by='Promedio General de sus Cursos',
                                                              ascending=False).round(2)

        st.dataframe(df_promedio_docente, use_container_width=True)
    elif not COLUMNAS_NOTAS:
        st.warning("No hay columnas de notas configuradas para calcular promedios.")
    else:
        st.info("Aún no hay datos para calcular el promedio general.")

    st.markdown('***')


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