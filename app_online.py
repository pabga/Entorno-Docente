import streamlit as st
import pandas as pd
import gspread
import numpy as np
import re
from gspread import utils

# --- CONFIGURACIÓN ---
DNI_ADMIN = "41209872"
CURSO_PRINCIPAL = 'CURSO_PRINCIPAL'


# --- CREDENCIALES ---
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


# --- CARGA DE DATOS ---
def load_data_online():
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        sh = gc.open_by_url(st.secrets["cursos_sheet_url"])

        def get_clean_df(sheet_name):
            try:
                ws = sh.worksheet(sheet_name)
                vals = ws.get_all_values()
                if not vals: return pd.DataFrame()
                headers = [str(h).strip().upper() for h in vals[0]]
                if len(vals) > 1:
                    return pd.DataFrame(vals[1:], columns=headers)
                return pd.DataFrame(columns=headers)
            except:
                return pd.DataFrame()

        df_al = get_clean_df("alumnos")
        df_cu = get_clean_df("cursos")
        df_in = get_clean_df("instructores")

        # --- CARGA NOTAS ---
        ws_notas = sh.worksheet("notas")
        raw_notas = ws_notas.get_all_values()

        if not raw_notas:
            return df_al, df_cu, pd.DataFrame(columns=['DNI', 'ID_CURSO', 'ROW_INDEX']), df_in

        headers = [str(h).strip().upper() for h in raw_notas[0]]

        # INYECCIÓN FORZADA DE NOMBRES
        if len(headers) >= 1: headers[0] = 'DNI'
        if len(headers) >= 2: headers[1] = 'ID_CURSO'
        if len(headers) == 1: headers.append('ID_CURSO')

        data = raw_notas[1:]
        if len(headers) > 1 and data and len(data[0]) < len(headers):
            data = [row + [""] * (len(headers) - len(row)) for row in data]

        df_no = pd.DataFrame(data, columns=headers)
        df_no['ROW_INDEX'] = [i + 2 for i in range(len(data))]

        # Detectar columnas de notas
        all_cols = [c for c in df_no.columns if c != 'ROW_INDEX']
        try:
            # Buscar cualquier variante de COMENTARIO
            idx_coment = next(i for i, c in enumerate(all_cols) if "COMENT" in c)
            notas_cols = all_cols[2:idx_coment] if len(all_cols) > 2 else []
        except:
            notas_cols = all_cols[2:] if len(all_cols) > 2 else []

        st.session_state['notas_header_list'] = notas_cols
        st.session_state['full_header_list'] = df_no.columns.tolist()

        return df_al, df_cu, df_no, df_in

    except Exception as e:
        st.error(f"❌ Error crítico en carga: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# --- PROCESAMIENTO ---
def procesar_datos(df_al, df_cu, df_no):
    if df_no is None or df_no.empty: return pd.DataFrame()

    if 'ID_CURSO' not in df_no.columns: df_no['ID_CURSO'] = "SIN_CURSO"
    if 'DNI' not in df_no.columns: df_no['DNI'] = "0"

    try:
        df_no['ID_CURSO'] = df_no['ID_CURSO'].astype(str).str.strip().str.upper()
        df_no['DNI'] = df_no['DNI'].astype(str).str.strip()
    except Exception as e:
        st.error(f"Error limpiando columnas clave: {e}")
        return pd.DataFrame()

    if not df_al.empty:
        if 'DNI' not in df_al.columns: df_al.rename(columns={df_al.columns[0]: 'DNI'}, inplace=True)
        df_al['DNI'] = df_al['DNI'].astype(str).str.strip()

    if not df_cu.empty:
        if 'D_CURSO' not in df_cu.columns: df_cu.rename(columns={df_cu.columns[0]: 'D_CURSO'}, inplace=True)
        df_cu['D_CURSO'] = df_cu['D_CURSO'].astype(str).str.strip().str.upper()

    if 'NOMBRE' in df_al.columns and 'APELLIDO' in df_al.columns:
        df = pd.merge(df_no, df_al[['DNI', 'NOMBRE', 'APELLIDO']], on='DNI', how='left')
    else:
        df = df_no.copy()
        df['NOMBRE'] = "---"
        df['APELLIDO'] = "---"

    if 'ASIGNATURA' in df_cu.columns:
        df = pd.merge(df, df_cu[['D_CURSO', 'ASIGNATURA']], left_on='ID_CURSO', right_on='D_CURSO', how='left')
    else:
        df['ASIGNATURA'] = df['ID_CURSO']

    df[CURSO_PRINCIPAL] = df['ID_CURSO'].apply(lambda x: x.split('-')[0] if '-' in str(x) else str(x))
    return df


# --- SINCRONIZACIÓN ---
def sincronizar_matriz_notas(df_alumnos, df_cursos, df_notas_actuales):
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        ws_notas = gc.open_by_url(st.secrets["cursos_sheet_url"]).worksheet("notas")

        dnis = []
        if not df_alumnos.empty:
            dnis = [str(x).strip() for x in df_alumnos.iloc[:, 0].unique() if str(x).strip()]

        cursos = []
        if not df_cursos.empty:
            cursos = [str(x).strip().upper() for x in df_cursos.iloc[:, 0].unique() if str(x).strip()]

        existentes = set()
        if not df_notas_actuales.empty and 'DNI' in df_notas_actuales.columns and 'ID_CURSO' in df_notas_actuales.columns:
            ex_dnis = df_notas_actuales['DNI'].astype(str).str.strip()
            ex_curs = df_notas_actuales['ID_CURSO'].astype(str).str.strip().str.upper()
            existentes = set(zip(ex_dnis, ex_curs))

        nuevas = []
        cols_totales = len(st.session_state.get('full_header_list', []))
        padding = cols_totales - 2
        if padding < 1: padding = 5

        for c in cursos:
            for a in dnis:
                if (a, c) not in existentes:
                    nuevas.append([a, c] + [""] * padding)

        if nuevas:
            ws_notas.append_rows(nuevas)
            st.success(f"✅ Creados {len(nuevas)} registros.")
            st.rerun()
        else:
            st.info("Todo al día.")
    except Exception as e:
        st.error(f"Error Sync: {e}")


# --- GUARDADO ---
def guardar_cambios(edited_df):
    if edited_df is None or edited_df.empty: return
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        ws = gc.open_by_url(st.secrets["cursos_sheet_url"]).worksheet("notas")

        headers = st.session_state['full_header_list']
        gs_headers = [h for h in headers if h != 'ROW_INDEX']

        batch = []
        cols_check = st.session_state.get('notas_header_list', [])
        col_com = next((c for c in edited_df.columns if "COMENT" in c), None)
        if col_com: cols_check.append(col_com)

        for i in range(len(edited_df)):
            fila = edited_df.iloc[i]
            if 'ROW_INDEX' not in fila: continue

            gs_row = int(fila['ROW_INDEX'])

            for col in cols_check:
                if col in fila and col in gs_headers:
                    val = fila[col]
                    if hasattr(val, "item"): val = val.item()
                    if pd.isna(val): val = ""
                    col_idx = gs_headers.index(col) + 1
                    batch.append({'range': utils.rowcol_to_a1(gs_row, col_idx), 'values': [[str(val)]]})

        if batch:
            ws.batch_update(batch)
            st.success("✅ Guardado.")
            st.rerun()
    except Exception as e:
        st.error(f"Error Guardar: {e}")


# --- INTERFAZ ---
st.set_page_config(page_title="Gestión de Notas", layout="wide")

df_al, df_cu, df_no, df_in = load_data_online()

if df_no is not None:
    df_final = procesar_datos(df_al, df_cu, df_no)
else:
    df_final = pd.DataFrame()

# --- LOGIN (CORREGIDO PARA DETECTAR ERROR DE TIPO) ---
if 'auth' not in st.session_state: st.session_state.auth = False

if not st.session_state.auth:
    st.title("Acceso Docente")
    u = st.text_input("DNI")
    p = st.text_input("Clave", type="password")

    if st.button("Ingresar"):
        if not df_in.empty:
            # 1. Copia de seguridad
            df_log = df_in.copy()

            # 2. Convertir TODO a String y quitar espacios
            # Iteramos por las columnas por índice para no depender de nombres
            if len(df_log.columns) >= 3:
                # Asumimos: Col 0 = DNI, Col 1 = Curso, Col 2 = Clave
                user_col = df_log.iloc[:, 0].astype(str).str.strip()
                pass_col = df_log.iloc[:, 2].astype(str).str.strip()
                curs_col = df_log.iloc[:, 1].astype(str).str.strip().str.upper()

                # 3. Comparación
                # Buscamos coincidencias exactas
                match = df_log[(user_col == u.strip()) & (pass_col == p.strip())]

                if not match.empty:
                    st.session_state.auth = True
                    st.session_state.dni = u
                    # Guardamos los cursos
                    st.session_state.cursos = [str(x).strip().upper() for x in match.iloc[:, 1].unique()]
                    st.rerun()
                else:
                    st.error("Datos incorrectos.")
                    # DEBUG VISUAL: Mostrar tabla para que veas qué está leyendo
                    with st.expander("🔍 Ver qué está leyendo el sistema (Debug)"):
                        st.write("Tu hoja de instructores se ve así para el sistema:")
                        st.dataframe(df_log)
                        st.write(f"Tú escribiste Usuario: '{u}' y Clave: '{p}'")
            else:
                st.error("La hoja de instructores tiene menos de 3 columnas.")
        else:
            st.error("Error cargando instructores.")

else:
    # --- APP PRINCIPAL ---
    sb = st.sidebar
    sb.write(f"Usuario: {st.session_state.dni}")

    if str(st.session_state.dni) == DNI_ADMIN:
        sb.info("Admin")
        if sb.button("🔄 Sincronizar Alumnos"):
            sincronizar_matriz_notas(df_al, df_cu, df_no)

    if sb.button("Salir"):
        st.session_state.auth = False
        st.rerun()

    if df_final.empty:
        st.warning("No hay notas cargadas.")
    else:
        mis = st.session_state.cursos
        # Filtro de seguridad
        if 'ID_CURSO' in df_final.columns:
            df_mio = df_final[df_final['ID_CURSO'].isin(mis)]

            if df_mio.empty:
                st.warning("Tus cursos no tienen alumnos asignados en la hoja de notas.")
            else:
                grps = sorted(df_mio[CURSO_PRINCIPAL].unique())
                tabs = st.tabs(grps)
                for i, g in enumerate(grps):
                    with tabs[i]:
                        dft = df_mio[df_mio[CURSO_PRINCIPAL] == g].reset_index(drop=True)

                        col_com = next((c for c in dft.columns if "COMENT" in c), None)
                        cols_n = st.session_state.get('notas_header_list', [])

                        base_cols = ['NOMBRE', 'APELLIDO', 'ID_CURSO']
                        extra_cols = cols_n + ([col_com] if col_com else [])

                        final_view = [c for c in base_cols + extra_cols if c in dft.columns]
                        final_edit = ['ROW_INDEX'] + final_view

                        cnf = {k: st.column_config.Column(disabled=True) for k in base_cols + ['ROW_INDEX']}

                        out = st.data_editor(dft[final_edit], column_config=cnf, key=f"e_{g}", hide_index=True,
                                             use_container_width=True)

                        if st.button(f"Guardar {g}", key=f"b_{g}"):
                            guardar_cambios(out)
        else:
            st.error("Error de estructura en datos procesados.")