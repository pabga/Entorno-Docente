import streamlit as st
import pandas as pd
import gspread
import numpy as np
import json
import re
from gspread import utils

# --- CONFIGURACIÓN DE COLUMNAS Y DATOS MAESTROS ---
ID_ALUMNO = 'DNI'
ID_CURSO_NOTAS = 'ID_CURSO'
ID_CURSO_MAESTRO = 'D_CURSO'
CURSO_PRINCIPAL = 'CURSO_PRINCIPAL'

DOCENTES_ASIGNADOS = {}
COLUMNAS_NOTAS = []


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

    # --- PASO CRÍTICO: CREAR EL CURSO PRINCIPAL ---
    def get_curso_principal(curso_id):
        if pd.isna(curso_id):
            return "OTROS"
        match = re.match(r'^([A-Z0-9]+)', curso_id)
        return match.group(1) if match else "OTROS"

    df_final[CURSO_PRINCIPAL] = df_final[ID_CURSO_NOTAS].apply(get_curso_principal)
    # ----------------------------------------------

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
#             FUNCIÓN: AÑADIR COLUMNA DE EVALUACIÓN (CORREGIDA)
# ----------------------------------------------------------------------

def add_new_exam_column(new_column_name):
    """Añade una nueva columna a la hoja 'notas' y refresca la aplicación."""
    try:
        if new_column_name in st.session_state['notas_columns']:
            st.warning(f"La columna '{new_column_name}' ya existe en la hoja de notas.")
            return

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

        # 1. Encontrar la columna 'Comentarios_Docente'
        try:
            # Obtener el índice de la columna en la lista de encabezados de la sesión
            comentarios_index = st.session_state['notas_columns'].index('Comentarios_Docente')
            # Insertar justo ANTES de Comentarios_Docente (el índice de columna es 1-based, no 0-based)
            insert_col_index = comentarios_index + 1
        except ValueError:
            # Si no existe, insertar al final
            insert_col_index = len(st.session_state['notas_columns']) + 1

        # 2. Insertar la nueva columna.
        # CORRECCIÓN: Usar solo el argumento 'values' correctamente anidado para el encabezado.
        worksheet.insert_cols(
            [[]],  # Lista vacía de filas para insertar
            col=insert_col_index,
            values=[[new_column_name]],  # Aquí debe ser [[Nombre]] para la primera celda
            inherit=False
        )

        st.success(f"✅ Columna '{new_column_name}' añadida exitosamente a la hoja de notas.")

        # 3. Forzar la recarga de datos
        st.cache_data.clear()
        st.rerun()

    except Exception as e:
        st.error(f"Error al añadir la columna. Asegúrese de que el nombre es válido. Detalle: {e}")


# ----------------------------------------------------------------------
#                         EJECUCIÓN PRINCIPAL Y STREAMLIT
# ----------------------------------------------------------------------

st.set_page_config(page_title="Dashboard de Notas Docente (Online)", layout="wide")

# --- Formulario de Agregar Examen en la Sidebar ---
st.sidebar.markdown('---')
st.sidebar.header("➕ Añadir Columna de Evaluación")
st.sidebar.warning("¡Esto modificará la hoja de 'notas' en Google Drive!")

with st.sidebar.form("add_exam_form"):
    exam_name = st.text_input("Nombre de la nueva Evaluación (Ej: Examen Final)", key="exam_name_input")
    submit_exam = st.form_submit_button("Añadir Columna")

    if submit_exam and exam_name:
        cleaned_exam_name = exam_name.strip()
        add_new_exam_column(cleaned_exam_name)
    elif submit_exam and not exam_name:
        st.sidebar.error("Ingrese un nombre para la evaluación.")
# --------------------------------------------------------------------


# 1. CARGA Y CÁLCULO INICIAL
if 'df_final_completo' not in st.session_state:
    with st.spinner("Cargando y validando datos desde Google Drive..."):
        df_alumnos_full, df_cursos_full, df_notas_brutas_full, df_instructores_full = load_data_online()

    if df_alumnos_full.empty or df_cursos_full.empty or df_notas_brutas_full.empty or df_instructores_full.empty:
        st.error("No se pudieron cargar todos los datos maestros (incluyendo la lista de instructores).")
        st.stop()

    # --- LIMPIEZA INICIAL ---
    try:
        def clean_code_column(df, col_name):
            if col_name in df.columns:
                df[col_name] = df[col_name].astype(str)
                df[col_name] = df[col_name].str.replace(r'[.,]', '', regex=True)
                df[col_name] = df[col_name].str.replace(r'\s+', '', regex=True)
            return df


        df_instructores_full = clean_code_column(df_instructores_full, 'DNI_DOCENTE')
        df_alumnos_full = clean_code_column(df_alumnos_full, 'DNI')
        df_notas_brutas_full = clean_code_column(df_notas_brutas_full, 'DNI')

        df_instructores_full = clean_code_column(df_instructores_full, 'ID_CURSO')
        df_notas_brutas_full = clean_code_column(df_notas_brutas_full, 'ID_CURSO')
        df_cursos_full = clean_code_column(df_cursos_full, 'D_CURSO')

        df_instructores_full['ID_CURSO'] = df_instructores_full['ID_CURSO'].str.upper()
        df_notas_brutas_full['ID_CURSO'] = df_notas_brutas_full['ID_CURSO'].str.upper()
        df_cursos_full['D_CURSO'] = df_cursos_full['D_CURSO'].str.upper()

    except Exception as e:
        st.error(f"Error al intentar limpiar y convertir columnas DNI/Curso a texto: {e}")
        st.stop()

    # --- PROCESAMIENTO DINÁMICO ---
    try:
        docentes_asignados_map = (
            df_instructores_full.groupby('DNI_DOCENTE')['ID_CURSO']
            .apply(list)
            .to_dict()
        )
        st.session_state['docentes_asignados_map'] = docentes_asignados_map

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

    cursos_asignados = [c.replace('.', '').replace(',', '').replace(' ', '').upper() for c in cursos_asignados_raw if
                        isinstance(c, str)]

    # 1. FILTRADO DEL DATAFRAME BASE COMPLETO
    df_final_completo = st.session_state['df_final_completo']

    # --- CÁLCULO DE PESTAÑAS DINÁMICAS (Cursos Principales) ---
    df_cursos_asignados_filtrado = df_final_completo[df_final_completo[ID_CURSO_NOTAS].isin(cursos_asignados)]
    cursos_principales_asignados = df_cursos_asignados_filtrado[CURSO_PRINCIPAL].unique().tolist()

    if not cursos_principales_asignados and cursos_asignados:
        cursos_principales_asignados = [c.split('-')[0] for c in cursos_asignados]
        cursos_principales_asignados = sorted(list(set(cursos_principales_asignados)))

    if not cursos_principales_asignados:
        st.warning(
            "Usted está asignado, pero sus cursos no se encontraron en el sistema de notas o no tienen código de curso principal legible.")
        return
    # -------------------------------------------------------------

    # --- DEBUGGING Y RAW DATA ---
    codigos_encontrados_en_notas = df_final_completo[ID_CURSO_NOTAS].unique().tolist()

    st.sidebar.markdown('---')
    st.sidebar.write("### 🚨 DEBUGGING CRÍTICO (RAW DATA)")
    st.sidebar.write("**1. Cursos Asignados (BÚSQUEDA LIMPIA):**")
    st.sidebar.code(repr(cursos_asignados))
    st.sidebar.write("**2. Códigos en Hoja NOTAS (EXISTENCIA LIMPIA):**")
    st.sidebar.code(repr(codigos_encontrados_en_notas))
    st.sidebar.write(f"Cursos Principales (PESTAÑAS): {cursos_principales_asignados}")
    st.sidebar.markdown('---')
    # --- FIN DEBUGGING ---

    st.title(f'👩‍🏫 Dashboard Docente - DNI: {docente_dni}')
    st.info(
        f"Mostrando datos agrupados por {len(cursos_principales_asignados)} Cursos Principales: {', '.join(cursos_principales_asignados)}")
    st.markdown('***')

    # -----------------------------------------------------------
    # INICIO DE ESTRUCTURA DE PESTAÑAS DINÁMICAS
    # -----------------------------------------------------------

    tabs = st.tabs(cursos_principales_asignados)

    for i, curso_principal_id in enumerate(cursos_principales_asignados):
        with tabs[i]:
            st.header(f"Agrupación: {curso_principal_id}")

            # --- 1. FILTRADO: SOLO EL CURSO PRINCIPAL ACTUAL ---

            df_filtrado_principal = df_final_completo[
                (df_final_completo[CURSO_PRINCIPAL] == curso_principal_id)
            ].copy()

            df_filtrado_docente = df_filtrado_principal[
                df_filtrado_principal[ID_CURSO_NOTAS].isin(cursos_asignados)
            ].reset_index(drop=True).copy()

            if df_filtrado_docente.empty:
                st.warning(
                    f"⚠️ No hay notas registradas en la hoja 'notas' para las submaterias asignadas de {curso_principal_id}.")
                continue

            if 'Comentarios_Docente' not in df_filtrado_docente.columns:
                df_filtrado_docente['Comentarios_Docente'] = ''

            # 2. INTERFAZ DE EDICIÓN
            columnas_visibles_editor = ['Nombre', 'Apellido', 'Asignatura', ID_CURSO_NOTAS] + COLUMNAS_NOTAS + [
                'Comentarios_Docente']

            st.subheader('📝 Edición de Notas (Submaterias Asignadas)')

            with st.form(f"notas_form_{curso_principal_id}"):

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
                    df_filtrado_docente[columnas_visibles_editor],
                    column_config=column_config_dict,
                    hide_index=False,
                    use_container_width=True,
                    key=f"editor_notas_{curso_principal_id}"
                )

                save_button = st.form_submit_button(f"💾 Guardar Cambios para {curso_principal_id}")

                if save_button:
                    edited_data = st.session_state[f"editor_notas_{curso_principal_id}"]
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