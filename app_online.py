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


@st.cache_data(ttl=300)
def load_data_online():
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        sh = gc.open_by_url(st.secrets["cursos_sheet_url"])

        # Función auxiliar para cargar y limpiar encabezados inmediatamente
        def get_df(sheet_name):
            data = sh.worksheet(sheet_name).get_all_records()
            df = pd.DataFrame(data)
            # Normalizar encabezados: Mayúsculas y sin espacios
            df.columns = [str(c).strip().upper() for c in df.columns]
            return df

        df_al = get_df("alumnos")
        df_cu = get_df("cursos")
        df_in = get_df("instructores")

        # Carga especial para notas (necesitamos ROW_INDEX)
        ws_notas = sh.worksheet("notas")
        raw_notas = ws_notas.get_all_records()
        if not raw_notas:
            st.error("⚠️ La hoja 'notas' está vacía o le faltan los encabezados DNI e ID_CURSO.")
            return None, None, None, None

        df_no = pd.DataFrame(raw_notas)
        df_no.columns = [str(c).strip().upper() for c in df_no.columns]
        df_no['ROW_INDEX'] = [i + 2 for i in range(len(raw_notas))]

        # Detectar columnas de notas (entre ID_CURSO y COMENTARIOS)
        all_cols = [c for c in df_no.columns.tolist() if c != 'ROW_INDEX']
        try:
            idx_start = all_cols.index('ID_CURSO') + 1
            idx_end = next(i for i, c in enumerate(all_cols) if "COMENTARIO" in c)
            notas_cols = all_cols[idx_start:idx_end]
        except:
            notas_cols = all_cols[2:-1] if len(all_cols) > 3 else []

        for c in notas_cols:
            df_no[c] = pd.to_numeric(df_no[c], errors='coerce').fillna(0)

        st.session_state['notas_header_list'] = notas_cols
        st.session_state['full_header_list'] = all_cols

        return df_al, df_cu, df_no, df_in
    except Exception as e:
        st.error(f"❌ Error en carga: {e}")
        return None, None, None, None


def sincronizar_matriz_notas(df_alumnos, df_cursos, df_notas_actuales):
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        ws_notas = gc.open_by_url(st.secrets["cursos_sheet_url"]).worksheet("notas")

        alumnos_dni = [str(d).strip() for d in df_alumnos['DNI'].unique()]
        cursos_id = [str(c).strip().upper() for c in df_cursos['D_CURSO'].unique()]

        existentes = set()
        if not df_notas_actuales.empty:
            existentes = set(zip(df_notas_actuales['DNI'].astype(str).str.strip(),
                                 df_notas_actuales['ID_CURSO'].astype(str).str.strip().upper()))

        nuevas_filas = []
        for c in cursos_id:
            for a in alumnos_dni:
                if (a, c) not in existentes:
                    # [DNI, ID_CURSO, notas... , comentario]
                    fila = [a, c] + [0] * len(st.session_state.get('notas_header_list', [])) + [""]
                    nuevas_filas.append(fila)

        if nuevas_filas:
            ws_notas.append_rows(nuevas_filas)
            st.success(f"✅ Sincronizado: {len(nuevas_filas)} filas nuevas.")
            st.cache_data.clear()
            st.rerun()
        else:
            st.info("ℹ️ Ya está todo sincronizado.")
    except Exception as e:
        st.error(f"❌ Error sincronizando: {e}")


def guardar_cambios(edited_df):
    if not isinstance(edited_df, pd.DataFrame) or edited_df.empty: return
    try:
        creds = get_gcp_credentials()
        gc = gspread.service_account_from_dict(creds)
        ws = gc.open_by_url(st.secrets["cursos_sheet_url"]).worksheet("notas")
        headers = st.session_state['full_header_list']

        batch = []
        col_coment = [c for c in edited_df.columns if "COMENTARIO" in c][0]
        columns_to_save = st.session_state['notas_header_list'] + [col_coment]

        for i in range(len(edited_df)):
            fila = edited_df.iloc[i]
            gs_row = int(fila['ROW_INDEX'])
            for col in columns_to_save:
                if col in fila:
                    val = fila[col]
                    if hasattr(val, "item"): val = val.item()  # Convertir int64 a int
                    if pd.isna(val): val = ""
                    col_gs = headers.index(col) + 1
                    batch.append({'range': utils.rowcol_to_a1(gs_row, col_gs), 'values': [[val]]})

        if batch:
            ws.batch_update(batch)
            st.success("✅ Guardado correctamente.")
            st.cache_data.clear()
            st.rerun()
    except Exception as e:
        st.error(f"❌ Error al guardar: {e}")


def procesar_datos(df_al, df_cu, df_no):
    if df_no is None or df_no.empty: return pd.DataFrame()

    # Cruce seguro (Todo ya está normalizado en load_data_online)
    df_no['DNI'] = df_no['DNI'].astype(str).str.strip()
    df_al['DNI'] = df_al['DNI'].astype(str).str.strip()
    df_no['ID_CURSO'] = df_no['ID_CURSO'].astype(str).str.strip().upper()
    df_cu['D_CURSO'] = df_cu['D_CURSO'].astype(str).str.strip().upper()

    df = pd.merge(df_no, df_al[['DNI', 'NOMBRE', 'APELLIDO']], on='DNI', how='left')
    df = pd.merge(df, df_cu[['D_CURSO', 'ASIGNATURA']], left_on='ID_CURSO', right_on='D_CURSO', how='left')

    def extraer_principal(val):
        res = re.match(r'^([A-Z0-9]+)', str(val))
        return res.group(1) if res else "OTROS"

    df[CURSO_PRINCIPAL] = df['ID_CURSO'].apply(extraer_principal)
    return df


# --- INTERFAZ ---
st.set_page_config(page_title="Sistema de Notas", layout="wide")
df_al, df_cu, df_no, df_in = load_data_online()
df_final = procesar_datos(df_al, df_cu, df_no)

if 'logeado' not in st.session_state: st.session_state.logeado = False

if not st.session_state.logeado:
    st.title("🔐 Acceso Sistema")
    dni_in = st.text_input("DNI")
    pass_in = st.text_input("Clave", type="password")
    if st.button("Entrar"):
        if df_in is not None and not df_in.empty:
            verif = df_in[(df_in['DNI_DOCENTE'].astype(str) == dni_in) & (df_in['CLAVE_ACCESO'].astype(str) == pass_in)]
            if not verif.empty:
                st.session_state.logeado = True
                st.session_state.dni = dni_in
                st.session_state.cursos = [str(c).upper().strip() for c in verif['ID_CURSO'].tolist()]
                st.rerun()
            else:
                st.error("Datos incorrectos.")
else:
    with st.sidebar:
        st.write(f"Sesión: {st.session_state.dni}")
        if str(st.session_state.dni) == DNI_ADMIN:
            st.markdown("---")
            st.subheader("🛠 Panel Admin")
            if st.button("🔄 Sincronizar Alumnos"):
                sincronizar_matriz_notas(df_al, df_cu, df_no)
        if st.button("Cerrar Sesión"):
            st.session_state.logeado = False
            st.rerun()

    if df_final.empty:
        st.warning("No hay datos cargados en la hoja de notas.")
    else:
        mis_cursos = st.session_state.cursos
        df_mio = df_final[df_final['ID_CURSO'].isin(mis_cursos)]
        grupos = sorted(df_mio[CURSO_PRINCIPAL].unique())

        if len(grupos) > 0:
            tabs = st.tabs(list(grupos))
            cols_n = st.session_state.get('notas_header_list', [])
            for i, g in enumerate(grupos):
                with tabs[i]:
                    df_tab = df_mio[df_mio[CURSO_PRINCIPAL] == g].reset_index(drop=True)
                    # Ocultar visualmente pero mantener ROW_INDEX para el guardado
                    col_coment = [c for c in df_tab.columns if "COMENTARIO" in c][0]
                    cols_show = ['NOMBRE', 'APELLIDO', 'ID_CURSO'] + cols_n + [col_coment]
                    cols_edit = ['ROW_INDEX'] + cols_show

                    config = {
                        "ROW_INDEX": st.column_config.Column(disabled=True, width="small"),
                        "NOMBRE": st.column_config.Column(disabled=True),
                        "APELLIDO": st.column_config.Column(disabled=True),
                        "ID_CURSO": st.column_config.Column(disabled=True)
                    }

                    edited_df = st.data_editor(df_tab[cols_edit], column_config=config, key=f"ed_{g}",
                                               use_container_width=True, hide_index=True)
                    if st.button(f"Guardar {g}", key=f"btn_{g}"):
                        guardar_cambios(edited_df)
        else:
            st.warning("No tienes materias asignadas con alumnos registrados.")