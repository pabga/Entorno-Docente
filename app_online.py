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


# HEMOS QUITADO EL CACHÉ PARA EVITAR ERRORES DE MEMORIA VIEJA
def load_data_online():
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        sh = gc.open_by_url(st.secrets["cursos_sheet_url"])

        def get_clean_df(sheet_name):
            ws = sh.worksheet(sheet_name)
            raw = ws.get_all_values()
            if not raw: return pd.DataFrame()
            headers = [str(h).strip().upper() for h in raw[0]]
            # Si hay datos
            if len(raw) > 1:
                return pd.DataFrame(raw[1:], columns=headers)
            return pd.DataFrame(columns=headers)

        df_al = get_clean_df("alumnos")
        df_cu = get_clean_df("cursos")
        df_in = get_clean_df("instructores")

        # --- CARGA DE NOTAS ---
        ws_notas = sh.worksheet("notas")
        raw_notas = ws_notas.get_all_values()

        if not raw_notas:
            st.error("⚠️ La hoja 'notas' está vacía.")
            return df_al, df_cu, pd.DataFrame(), df_in

        headers = [str(h).strip().upper() for h in raw_notas[0]]
        data = raw_notas[1:]

        df_no = pd.DataFrame(data, columns=headers)

        # --- INTENTO DE RENOMBRADO PREVENTIVO ---
        cols = list(df_no.columns)
        # Si hay al menos 2 columnas, asumimos que la B es ID_CURSO
        if len(cols) >= 2:
            cols[1] = 'ID_CURSO'
            # Solo renombramos si la columna 0 no es ROW_INDEX (por seguridad)
            if cols[0] != 'ROW_INDEX': cols[0] = 'DNI'
            df_no.columns = cols

        # Añadir índice de fila
        df_no['ROW_INDEX'] = [i + 2 for i in range(len(data))]

        # Detectar columnas de notas
        all_cols = [c for c in df_no.columns if c != 'ROW_INDEX']

        # Buscar "COMENTARIO"
        idx_coment = -1
        for i, cname in enumerate(all_cols):
            if "COMENTARIO" in cname:
                idx_coment = i
                break

        # Definir rango de notas
        if idx_coment != -1:
            # Asumimos que notas empiezan en columna 2 (despues de DNI e ID_CURSO)
            notas_cols = all_cols[2:idx_coment]
        else:
            notas_cols = all_cols[2:]

        # Limpiar notas a números
        for c in notas_cols:
            df_no[c] = pd.to_numeric(df_no[c], errors='coerce').fillna(0)

        st.session_state['notas_header_list'] = notas_cols
        st.session_state['full_header_list'] = df_no.columns.tolist()

        return df_al, df_cu, df_no, df_in

    except Exception as e:
        st.error(f"❌ Error en carga: {e}")
        return None, None, None, None


def sincronizar_matriz_notas(df_alumnos, df_cursos, df_notas_actuales):
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        ws_notas = gc.open_by_url(st.secrets["cursos_sheet_url"]).worksheet("notas")

        # Extraer DNI y Cursos limpios
        dni_list = []
        if 'DNI' in df_alumnos.columns:
            dni_list = [str(d).strip() for d in df_alumnos['DNI'].unique() if str(d).strip()]

        curso_list = []
        # Buscar columna de ID Curso en cursos
        col_id_curso = 'D_CURSO' if 'D_CURSO' in df_cursos.columns else df_cursos.columns[0]
        curso_list = [str(c).strip().upper() for c in df_cursos[col_id_curso].unique() if str(c).strip()]

        # Verificar existentes (Usando iloc para evitar error de nombre de columna)
        existentes = set()
        if not df_notas_actuales.empty and len(df_notas_actuales.columns) >= 2:
            dnis_ex = df_notas_actuales.iloc[:, 0].astype(str).str.strip()
            cursos_ex = df_notas_actuales.iloc[:, 1].astype(str).str.strip().upper()
            existentes = set(zip(dnis_ex, cursos_ex))

        nuevas_filas = []
        # Padding de columnas vacías
        num_cols = len(st.session_state.get('full_header_list', []))
        padding_count = num_cols - 2 if num_cols > 2 else 5

        for c in curso_list:
            for a in dni_list:
                if (a, c) not in existentes:
                    fila = [a, c] + [""] * padding_count
                    nuevas_filas.append(fila)

        if nuevas_filas:
            ws_notas.append_rows(nuevas_filas)
            st.success(f"✅ Sincronizado: {len(nuevas_filas)} registros nuevos.")
            st.rerun()
        else:
            st.info("Todo sincronizado.")
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
        cols_check = st.session_state.get('notas_header_list', [])
        # Agregar comentario a la lista de guardado
        col_com = next((c for c in edited_df.columns if "COMENTARIO" in c), None)
        if col_com: cols_check.append(col_com)

        for i in range(len(edited_df)):
            fila = edited_df.iloc[i]
            gs_row = int(fila['ROW_INDEX'])

            for col in cols_check:
                if col in fila and col in headers:
                    val = fila[col]
                    if hasattr(val, "item"): val = val.item()
                    if pd.isna(val): val = ""
                    col_gs = headers.index(col) + 1
                    batch.append({'range': utils.rowcol_to_a1(gs_row, col_gs), 'values': [[str(val)]]})

        if batch:
            ws.batch_update(batch)
            st.success("✅ Guardado.")
            st.rerun()
    except Exception as e:
        st.error(f"Error Guardar: {e}")


def procesar_datos(df_al, df_cu, df_no):
    # 1. Validación inicial
    if df_no is None or df_no.empty:
        return pd.DataFrame()

    # 2. REPARACIÓN DE EMERGENCIA DE COLUMNAS
    # Si 'ID_CURSO' no está, miramos si hay al menos 2 columnas y renombramos la 2da
    if 'ID_CURSO' not in df_no.columns:
        cols = list(df_no.columns)
        if len(cols) >= 2:
            cols[1] = 'ID_CURSO'  # Forzamos la columna B
            cols[0] = 'DNI'  # Forzamos la columna A
            df_no.columns = cols
        else:
            # Si solo hay 1 columna o 0, creamos la columna ID_CURSO vacía para NO ROMPER
            df_no['ID_CURSO'] = "SIN_CURSO"
            if 'DNI' not in df_no.columns:
                df_no['DNI'] = "SIN_DNI"
            st.warning("⚠️ Advertencia: La hoja de notas tiene formato incorrecto. Se crearon columnas temporales.")

    # 3. Conversión de Tipos (Ahora seguro porque las columnas existen sí o sí)
    df_no['ID_CURSO'] = df_no['ID_CURSO'].astype(str).str.strip().upper()
    df_no['DNI'] = df_no['DNI'].astype(str).str.strip()

    # 4. Normalizar Alumnos
    if not df_al.empty:
        if 'DNI' not in df_al.columns:  # Si alumno no tiene DNI, intentamos col 0
            df_al.rename(columns={df_al.columns[0]: 'DNI'}, inplace=True)
        df_al['DNI'] = df_al['DNI'].astype(str).str.strip()

    # 5. Normalizar Cursos
    col_id_maestro = 'D_CURSO'
    if not df_cu.empty:
        if 'D_CURSO' not in df_cu.columns:
            # Usamos la 1ra columna de cursos como ID
            col_id_maestro = df_cu.columns[0]
            df_cu.rename(columns={col_id_maestro: 'D_CURSO'}, inplace=True)
            col_id_maestro = 'D_CURSO'
        df_cu[col_id_maestro] = df_cu[col_id_maestro].astype(str).str.strip().upper()

    # 6. Merges (Cruces)
    # Cruce con Alumnos
    if 'NOMBRE' in df_al.columns and 'APELLIDO' in df_al.columns:
        df = pd.merge(df_no, df_al[['DNI', 'NOMBRE', 'APELLIDO']], on='DNI', how='left')
    else:
        df = df_no.copy()
        df['NOMBRE'] = df['DNI']
        df['APELLIDO'] = ""

    # Cruce con Cursos
    if 'ASIGNATURA' in df_cu.columns:
        df = pd.merge(df, df_cu[['D_CURSO', 'ASIGNATURA']], left_on='ID_CURSO', right_on='D_CURSO', how='left')
    else:
        df['ASIGNATURA'] = df['ID_CURSO']

    # 7. Crear columna principal para agrupar
    def get_main(x):
        return x.split('-')[0] if '-' in x else x

    df[CURSO_PRINCIPAL] = df['ID_CURSO'].apply(get_main)

    return df


# --- UI PRINCIPAL ---
st.set_page_config(page_title="Sistema Notas", layout="wide")

# Carga de datos
df_al, df_cu, df_no, df_in = load_data_online()

# Procesamiento (Ahora blindado contra errores)
if df_no is not None:
    df_final = procesar_datos(df_al, df_cu, df_no)
else:
    df_final = pd.DataFrame()

# Lógica de Sesión
if 'auth' not in st.session_state: st.session_state.auth = False

if not st.session_state.auth:
    st.title("Acceso")
    u = st.text_input("DNI")
    p = st.text_input("Clave", type="password")
    if st.button("Ingresar"):
        if df_in is not None and not df_in.empty:
            # Intentamos normalizar instructores
            if len(df_in.columns) >= 3:
                # Asumimos Col 0: DNI, Col 1: ID_CURSO, Col 2: CLAVE
                u_real = df_in.iloc[:, 0].astype(str).str.strip()
                p_real = df_in.iloc[:, 2].astype(str).str.strip()
                match = df_in[(u_real == u) & (p_real == p)]

                if not match.empty:
                    st.session_state.auth = True
                    st.session_state.dni = u
                    st.session_state.cursos = [str(c).upper().strip() for c in match.iloc[:, 1].tolist()]
                    st.rerun()
                else:
                    st.error("Datos incorrectos.")
            else:
                st.error("Error en estructura hoja instructores.")
        else:
            st.error("No se cargaron instructores.")
else:
    sb = st.sidebar
    sb.title(f"Usuario: {st.session_state.dni}")

    if str(st.session_state.dni) == DNI_ADMIN:
        sb.info("Modo Admin")
        if sb.button("Sincronizar Alumnos"):
            sincronizar_matriz_notas(df_al, df_cu, df_no)

    if sb.button("Salir"):
        st.session_state.auth = False
        st.rerun()

    if df_final.empty:
        st.warning("No hay notas disponibles.")
    else:
        mis = st.session_state.cursos
        df_mio = df_final[df_final['ID_CURSO'].isin(mis)]

        if df_mio.empty:
            st.warning("No tienes cursos asignados o tus cursos no tienen alumnos cargados en la hoja de notas.")
        else:
            grps = sorted(df_mio[CURSO_PRINCIPAL].unique())
            if not grps:
                st.write("Sin grupos.")
            else:
                tabs = st.tabs(grps)
                for i, g in enumerate(grps):
                    with tabs[i]:
                        dft = df_mio[df_mio[CURSO_PRINCIPAL] == g].reset_index(drop=True)

                        col_com = next((c for c in dft.columns if "COMENTARIO" in c), None)
                        cols_n = st.session_state.get('notas_header_list', [])

                        cols_ver = ['NOMBRE', 'APELLIDO', 'ID_CURSO'] + cols_n + ([col_com] if col_com else [])
                        # Filtramos solo columnas existentes
                        cols_ver = [c for c in cols_ver if c in dft.columns]
                        cols_edit = ['ROW_INDEX'] + cols_ver

                        cnf = {k: st.column_config.Column(disabled=True) for k in
                               ['ROW_INDEX', 'NOMBRE', 'APELLIDO', 'ID_CURSO']}

                        out = st.data_editor(dft[cols_edit], column_config=cnf, key=f"e_{g}", hide_index=True,
                                             use_container_width=True)

                        if st.button(f"Guardar {g}", key=f"b_{g}"):
                            guardar_cambios(out)