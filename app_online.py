import streamlit as st
import pandas as pd
import gspread
import numpy as np
from gspread import utils  # Utilidad para convertir coordenadas A1

# --- CONFIGURACIÓN DE COLUMNAS Y DATOS MAESTROS ---
COLUMNAS_NOTAS = ['Parcial 1', 'Parcial 2']
ID_ALUMNO = 'DNI'
ID_CURSO = 'ID_CURSO'

# Base de datos de docentes con su DNI y sus cursos asignados (Simulación/Lectura)
# NOTA: En la implementación final, esta asignación podría leerse de una hoja de 'Instructores'
DOCENTES_ASIGNADOS = {
    '17999767': ['MMA-FH-2025-P', 'MMA-FH-2025-F'],  # Docente 1
    '27642905': ['AER-MAT-2025-P'],  # Docente 2
    '22041283': ['MMA-FH-2025-P', 'MMA-FH-2025-F']  # Docente 3
}


# ----------------------------------------------------------------------
#                         CONEXIÓN Y CARGA DE DATOS REALES
# ----------------------------------------------------------------------

@st.cache_data(ttl=600)  # Cacha los datos por 10 minutos
def load_data_online():
    """
    Carga los DataFrames de Google Sheets usando la conexión segura.
    """
    try:
        # Usamos la conexión de Streamlit con la clave 'gcp_service_account'
        conn = st.connection("gcp_service_account", type="spreadsheet")

        url_cursos = st.secrets.cursos_sheet.url
        url_notas = st.secrets.notas_sheet.url

        # 1. Archivo Base_Datos_Cursos (Lectura de dos hojas)
        # Nota: La columna Comentarios_Docente es crucial en df_notas_brutas
        df_alumnos = conn.read(spreadsheet=url_cursos, worksheet="alumnos")
        df_cursos = conn.read(spreadsheet=url_cursos, worksheet="cursos")

        # 2. Archivo Notas_Docentes (Lectura de la hoja de notas)
        df_notas_brutas = conn.read(spreadsheet=url_notas, worksheet="notas")

        # Limpieza básica para asegurar que las notas sean numéricas
        cols_para_limpiar = COLUMNAS_NOTAS
        df_notas_brutas[cols_para_limpiar] = df_notas_brutas[cols_para_limpiar].apply(pd.to_numeric,
                                                                                      errors='coerce').fillna(0)

        # Guardar las columnas originales de df_notas_brutas para el mapeo de guardado
        st.session_state['notas_columns'] = df_notas_brutas.columns.tolist()

        return df_alumnos, df_cursos, df_notas_brutas

    except Exception as e:
        st.error(f"Error de conexión con Google Sheets. Revise 'secrets.toml' y permisos. Detalle: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# ----------------------------------------------------------------------
#                         FUNCIÓN DE CÁLCULO Y INTEGRACIÓN
# ----------------------------------------------------------------------

def integrar_y_calcular(df_alumnos, df_cursos, df_notas):
    """
    Integra los datos de Alumnos, Cursos y Notas, y calcula el promedio por materia.
    """

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
    """
    Identifica los cambios en el editor y los escribe en la Hoja de Google de Notas.
    """
    if not edited_data['edited_rows']:
        st.warning("No se detectaron cambios para guardar.")
        return

    try:
        # Usar las credenciales de secrets.toml
        gc = gspread.service_account_from_dict(st.secrets.gcp_service_account)
        spreadsheet = gc.open_by_url(st.secrets.notas_sheet.url)
        worksheet = spreadsheet.worksheet("notas")

        updates = []
        original_cols = st.session_state['notas_columns']  # Columnas de la hoja de notas

        # 1. Iterar sobre las filas modificadas
        for df_index, col_updates in edited_data['edited_rows'].items():

            # gsheet_row_index es el índice de la fila en GSheet (índice 0-based de Pandas + 2)
            gsheet_row_index = df_index + 2

            for col_name, new_value in col_updates.items():

                # Buscamos la posición de la columna en la hoja de notas original
                try:
                    col_index = original_cols.index(col_name) + 1  # +1 porque GSheet es 1-indexado
                except ValueError:
                    # Ignorar columnas que no existen en la hoja de notas original (ej: Nombre, Apellido, Asignatura)
                    continue

                    # Crear la celda A1 (Ej: 'C3')
                cell_a1 = utils.rowcol_to_a1(gsheet_row_index, col_index)

                # Añadir la actualización: (rango, valor)
                updates.append({
                    'range': cell_a1,
                    'values': [[new_value]]
                })

        # 2. Enviar todas las actualizaciones
        if updates:
            worksheet.batch_update(updates)
            st.success(f"💾 ¡{len(updates)} cambios guardados exitosamente en Google Sheets!")

            # Borramos el cache y forzamos la recarga para que el docente vea los datos actualizados
            st.cache_data.clear()
            st.rerun()

    except Exception as e:
        st.error(f"Error crítico al guardar. Revise permisos de Editor. Detalle: {e}")


# ----------------------------------------------------------------------
#                         EJECUCIÓN PRINCIPAL Y STREAMLIT
# ----------------------------------------------------------------------

st.set_page_config(page_title="Dashboard de Notas Docente (Online)", layout="wide")

# 1. CARGA Y CÁLCULO INICIAL (Se ejecuta y guarda en session_state)
if 'df_final_completo' not in st.session_state:
    df_alumnos_full, df_cursos_full, df_notas_brutas_full = load_data_online()
    if df_alumnos_full.empty or df_cursos_full.empty or df_notas_brutas_full.empty:
        st.error("No se pudieron cargar los datos maestros. Deteniendo la aplicación.")
        st.stop()

    df_final_full = integrar_y_calcular(df_alumnos_full, df_cursos_full, df_notas_brutas_full)
    st.session_state['df_final_completo'] = df_final_full
    st.session_state['df_notas_base'] = df_notas_brutas_full  # Guardar la base de notas para el guardado

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

    # 2. INTERFAZ DE EDICIÓN CON BOTÓN DE GUARDAR

    # Columnas que son visibles en el editor
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
                # Aseguramos que las notas sean numéricas y limitadas
                "Parcial 1": st.column_config.NumberColumn("Parcial 1", min_value=0.0, max_value=10.0, format="%.1f"),
                "Parcial 2": st.column_config.NumberColumn("Parcial 2", min_value=0.0, max_value=10.0, format="%.1f"),
                "Comentarios_Docente": st.column_config.TextColumn("Descripción/Comentario (Editable)"),
                # Hacemos las columnas de info del alumno solo lectura
                "Nombre": st.column_config.TextColumn("Nombre", disabled=True),
                "Apellido": st.column_config.TextColumn("Apellido", disabled=True),
                "Asignatura": st.column_config.TextColumn("Asignatura", disabled=True),
                "ID_CURSO": st.column_config.TextColumn("ID_CURSO", disabled=True),
            },
            # Necesitamos el índice de Streamlit (0, 1, 2...) para mapear la fila a la GSheet
            hide_index=False,
            use_container_width=True,
            key="editor_notas"
        )

        save_button = st.form_submit_button("💾 Guardar Cambios en Google Sheets")

        if save_button:
            # Capturamos los datos editados
            edited_data = st.session_state["editor_notas"]
            # Guardamos usando el DataFrame base de notas (st.session_state['df_notas_base'])
            # para que la función sepa dónde buscar DNI y ID_CURSO.
            save_data_to_gsheet(st.session_state['df_notas_base'], edited_data)

    st.markdown('***')

    # 3. RECALCULO DE PROMEDIOS CON LOS DATOS EDITADOS

    # Creamos un DataFrame para calcular el promedio (basado en el output del editor)
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