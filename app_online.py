import streamlit as st
import pandas as pd
import gspread
import numpy as np
from gspread import utils  # Utilidad para convertir coordenadas A1

# --- CONFIGURACIÓN DE COLUMNAS Y DATOS MAESTROS ---
COLUMNAS_NOTAS = ['Parcial 1', 'Parcial 2']
ID_ALUMNO = 'DNI'
ID_CURSO = 'ID_CURSO'
# Nota: 'Comentarios_Docente' se añade a las notas en la hoja de Drive.

# Base de datos de docentes con su DNI y sus cursos asignados (Lectura de la hoja 'cursos')
# ESTO ES MANUAL PARA EL DEMO. En producción, podrías leer esto de una hoja de instructores.
DOCENTES_ASIGNADOS = {
    '17999767': ['MMA-FH-2025-P', 'MMA-FH-2025-F'],  # Graciela
    '27642905': ['AER-MAT-2025-P'],  # Rodolfo
    '22041283': ['MMA-FH-2025-P', 'MMA-FH-2025-F']  # Daniel
}


# ----------------------------------------------------------------------
#                         CONEXIÓN Y CARGA DE DATOS REALES
# ----------------------------------------------------------------------

@st.cache_data(ttl=600)  # Cacha los datos por 10 minutos
def load_data_online():
    """
    Carga los DataFrames de Google Sheets usando gspread y st.secrets (Streamlit Cloud).
    """
    try:
        # 1. RECONSTRUCCIÓN DEL DICCIONARIO DE CREDENCIALES
        # Lee las claves individuales de Streamlit Cloud Secrets
        gcp_service_account_dict = {
            "type": st.secrets["gcp_service_account_type"],
            "project_id": st.secrets["gcp_service_account_project_id"],
            "private_key_id": st.secrets["gcp_service_account_private_key_id"],
            "private_key": st.secrets["gcp_service_account_private_key"],
            "client_email": st.secrets["gcp_service_account_client_email"],
            "client_id": st.secrets["gcp_service_account_client_id"],
            # Añadir todas las demás claves necesarias de tu JSON de servicio
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": st.secrets["gcp_service_account_client_x509_cert_url"]
        }

        # 2. AUTENTICACIÓN
        gc = gspread.service_account_from_dict(gcp_service_account_dict)

        # 3. ACCESO A URLs
        url_cursos = st.secrets["cursos_sheet_url"]
        url_notas = st.secrets["notas_sheet_url"]

        # 4. LECTURA DE DATOS
        cursos_sheet = gc.open_by_url(url_cursos)
        notas_sheet = gc.open_by_url(url_notas)

        # .get_all_records() lee los datos como un diccionario (con encabezado como clave)
        df_alumnos = pd.DataFrame(cursos_sheet.worksheet("alumnos").get_all_records())
        df_cursos = pd.DataFrame(cursos_sheet.worksheet("cursos").get_all_records())
        df_notas_brutas = pd.DataFrame(notas_sheet.worksheet("notas").get_all_records())

        # Limpieza: Asegurar que las columnas de notas sean numéricas y sin NaN
        cols_para_limpiar = COLUMNAS_NOTAS
        df_notas_brutas[cols_para_limpiar] = df_notas_brutas[cols_para_limpiar].apply(pd.to_numeric,
                                                                                      errors='coerce').fillna(0)

        # Guardar las columnas originales de df_notas_brutas para el mapeo de guardado
        st.session_state['notas_columns'] = df_notas_brutas.columns.tolist()

        return df_alumnos, df_cursos, df_notas_brutas

    except gspread.exceptions.APIError as api_e:
        st.error(
            f"Error de API de Google (Permisos/Conexión). Asegúrese de que el correo de la Cuenta de Servicio tiene acceso de Lector/Editor a los archivos. Detalle: {api_e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        st.error(
            f"Error al cargar los datos. Verifique las URLs, los nombres de las hojas o el formato de las credenciales en Streamlit Secrets. Detalle: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# ----------------------------------------------------------------------
#                         FUNCIÓN DE CÁLCULO Y INTEGRACIÓN
# ----------------------------------------------------------------------

def integrar_y_calcular(df_alumnos, df_cursos, df_notas):
    """Integra y calcula el promedio por materia."""

    if df_alumnos.empty or df_cursos.empty or df_notas.empty:
        return pd.DataFrame()

    # 1. Unir Notas con Alumnos
    df_paso1 = pd.merge(
        df_notas,
        df_alumnos[[ID_ALUMNO, 'Nombre', 'Apellido']],
        on=ID_ALUMNO,
        how='left'
    )

    # 2. Unir con Cursos
    df_final = pd.merge(
        df_paso1,
        df_cursos[['D_CURSO', 'Asignatura']],
        left_on='ID_CURSO',
        right_on='D_CURSO',
        how='left'
    )

    df_final.drop(columns=['D_CURSO'], inplace=True)

    # 3. CÁLCULO DE PROMEDIOS
    df_final['Promedio_Materia'] = df_final[COLUMNAS_NOTAS].mean(axis=1).round(2)

    return df_final


# ----------------------------------------------------------------------
#                         FUNCIÓN DE GUARDADO PERSISTENTE (GSPREAD)
# ----------------------------------------------------------------------

def save_data_to_gsheet(df_original_notas_base, edited_data):
    """Identifica los cambios en el editor y los escribe en la Hoja de Google de Notas."""

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
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": st.secrets["gcp_service_account_client_x509_cert_url"]
        }

        gc = gspread.service_account_from_dict(gcp_service_account_dict)
        spreadsheet = gc.open_by_url(st.secrets["notas_sheet_url"])
        worksheet = spreadsheet.worksheet("notas")

        updates = []
        original_cols = st.session_state['notas_columns']

        # 1. Iterar sobre las filas modificadas
        for df_index, col_updates in edited_data['edited_rows'].items():

            # gsheet_row_index es el índice de la fila en GSheet (índice 0-based de Pandas + 2)
            gsheet_row_index = df_index + 2

            for col_name, new_value in col_updates.items():

                # Omitir columnas que no están en la hoja de notas original
                if col_name not in original_cols:
                    continue

                # Encontrar el índice de la columna en GSheet
                col_index = original_cols.index(col_name) + 1

                # Crear el rango A1 para la celda a actualizar
                cell_a1 = utils.rowcol_to_a1(gsheet_row_index, col_index)

                updates.append({
                    'range': cell_a1,
                    'values': [[new_value]]
                })

        # 2. Enviar todas las actualizaciones
        if updates:
            worksheet.batch_update(updates)
            st.success(f"💾 ¡{len(updates)} cambios guardados exitosamente en Google Sheets!")

            # Limpiar cache y forzar recarga para ver los datos frescos de Drive
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
    st.session_state['df_final_completo'] = df_final_full
    st.session_state['df_notas_base'] = df_notas_brutas_full

# Inicializar el estado de sesión (autenticación)
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
    st.session_state['docente_dni'] = None


# --- Función de Login (Sin Cambios) ---
def login_form():
    st.sidebar.header("Inicio de Sesión")

    with st.sidebar.form("login_form"):
        dni_input = st.text_input("DNI del Docente (Ej: 17999767)", value="")
        password_input = st.text_input("Contraseña (Ej: 1234)", type="password")
        submitted = st.form_submit_button("Ingresar")

        if submitted:
            if dni_input in DOCENTES_ASIGNADOS and password_input == "1234":
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
        df_final_completo[ID_CURSO].isin(cursos_asignados)
    ].reset_index(drop=True).copy()

    # Asegúrate de que todas las columnas que necesitas editar estén presentes
    if 'Comentarios_Docente' not in df_filtrado_docente_base.columns:
        df_filtrado_docente_base['Comentarios_Docente'] = ''

    # 2. INTERFAZ DE EDICIÓN CON BOTÓN DE GUARDAR

    columnas_visibles_editor = ['Nombre', 'Apellido', 'Asignatura', 'ID_CURSO'] + COLUMNAS_NOTAS + [
        'Comentarios_Docente']

    st.header('📝 Edición de Notas y Comentarios')
    st.warning(
        "🚨 **Edite directamente las notas y comentarios. Use el botón 'Guardar Cambios' para persistir la data en Drive.**")

    # Creamos un formulario para agrupar el editor y el botón
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
                "ID_CURSO": st.column_config.TextColumn("ID_CURSO", disabled=True),
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