import streamlit as st
import pandas as pd
import numpy as np
import io
import gc
import time
import plotly.express as px

st.set_page_config(page_title="Dashboard Analítico de Precios", layout="wide")

st.title("Procesador y Dashboard de Precios")
st.write("Sube tu archivo para estructurar los datos y explorar el comportamiento de los precios.")

@st.cache_data(show_spinner=False)
def convert_df_to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    return output.getvalue()

@st.cache_data(show_spinner=False)
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

# --- FUNCIÓN DE PROCESAMIENTO ---
def procesar_archivo(file_buffer, file_name, progress_bar, status_text):
    status_text.text("Fase 1/5: Leyendo archivo...")
    if file_name.endswith('.csv'):
        df = pd.read_csv(file_buffer, encoding='utf-8', on_bad_lines='skip')
    else:
        df = pd.read_excel(file_buffer, engine='calamine')
        
    columnas_originales = df.columns.tolist()
    progress_bar.progress(20)

    status_text.text("Fase 2/5: Optimizando memoria del sistema...")
    columnas_texto = [
        'DESCRIPCION_ACTIVIDAD', 'CODIGO', 'CODIGO_OSINERG', 'NMBRE_UNDAD',
        'RUC', 'NOMDEPA', 'NOMPROV', 'NOMDIST', 'DIRECCION', 'DESCRIPCION_PRODUCTO', 'NRO_RGSTRO'
    ]
    for col in columnas_texto:
        if col in df.columns:
            df[col] = df[col].astype('category')
    
    if 'PRECIO_VENTA' in df.columns:
        df['PRECIO_VENTA'] = pd.to_numeric(df['PRECIO_VENTA'], errors='coerce')
    
    df['FECHA_REGISTRO_DT'] = pd.to_datetime(df['FECHA_REGISTRO'], format='%d/%m/%Y', errors='coerce')
    df['orden_original'] = np.arange(len(df), dtype=np.float64)
    progress_bar.progress(40)

    status_text.text("Fase 3/5: Evaluando saltos temporales entre fechas...")
    columnas_sort = ['CODIGO_OSINERG', 'DESCRIPCION_PRODUCTO', 'FECHA_REGISTRO_DT', 'orden_original']
    df_sorted = df.sort_values(by=columnas_sort)

    df_sorted['next_date'] = df_sorted.groupby(['CODIGO_OSINERG', 'DESCRIPCION_PRODUCTO'], observed=True)['FECHA_REGISTRO_DT'].shift(-1)
    df_sorted['days_diff'] = (df_sorted['next_date'] - df_sorted['FECHA_REGISTRO_DT']).dt.days

    gaps = df_sorted[df_sorted['days_diff'] > 1].copy()
    
    del df_sorted
    gc.collect()
    progress_bar.progress(60)

    status_text.text("Fase 4/5: Generando días faltantes...")
    if not gaps.empty:
        gaps['num_new_rows'] = (gaps['days_diff'] - 1).astype(np.float64).clip(upper=15).astype(np.int32)
        
        new_rows = gaps.loc[gaps.index.repeat(gaps['num_new_rows'])].copy()
        new_rows['add_days'] = new_rows.groupby(level=0).cumcount() + 1

        nueva_fecha_dt = new_rows['FECHA_REGISTRO_DT'] + pd.to_timedelta(new_rows['add_days'], unit='D')
        new_rows['FECHA_REGISTRO_DT'] = nueva_fecha_dt
        new_rows['FECHA_REGISTRO'] = nueva_fecha_dt.dt.strftime('%d/%m/%Y')

        new_rows['DIA'] = nueva_fecha_dt.dt.day
        new_rows['MES'] = nueva_fecha_dt.dt.month
        new_rows['ANIO'] = nueva_fecha_dt.dt.year
        new_rows['HORA_REGISTRO'] = ""

        new_rows['orden_original'] = new_rows['orden_original'] + (new_rows['add_days'] / 1000000.0)
        new_rows = new_rows.drop(columns=['next_date', 'days_diff', 'num_new_rows', 'add_days'])

        df_final = pd.concat([df.drop(columns=['next_date', 'days_diff'], errors='ignore'), new_rows], ignore_index=True)
        del new_rows
        del gaps
    else:
        df_final = df.copy()

    del df
    gc.collect()
    progress_bar.progress(80)

    status_text.text("Fase 5/5: Ordenando y limpiando la estructura final...")
    df_final = df_final.sort_values(by=columnas_sort, ascending=[True, True, False, False]).reset_index(drop=True)
    df_final = df_final.drop(columns=['orden_original'], errors='ignore')
    progress_bar.progress(100)
    
    return df_final

# --- INTERFAZ PRINCIPAL ---
uploaded_file = st.file_uploader("Sube tu archivo", type=['xlsx', 'xls', 'csv'])

if uploaded_file is not None:
    if 'procesado' not in st.session_state:
        st.session_state['procesado'] = False

    if not st.session_state['procesado']:
        col1, col2 = st.columns(2)
        if col1.button("▶️ Procesar Datos", type="primary"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            try:
                st.session_state['df_final'] = procesar_archivo(uploaded_file, uploaded_file.name, progress_bar, status_text)
                st.session_state['procesado'] = True
                
                # --- NUEVO: Guardamos el nombre original ---
                st.session_state['nombre_original'] = uploaded_file.name 
                
                status_text.success("¡Datos procesados con éxito!")
                time.sleep(1.5) 
                st.rerun()
            except Exception as e:
                status_text.error(f"Error durante el procesamiento: {e}")
                progress_bar.empty()
        
        if col2.button("⏹️ Cancelar"):
            st.stop()

# --- MÓDULO DE ANÁLISIS ---
if st.session_state.get('procesado', False) and 'df_final' in st.session_state:
    df_analisis = st.session_state['df_final']
    
    if st.button("🔄 Cargar un archivo nuevo"):
        st.session_state.clear() # Limpiamos toda la sesión de forma segura
        st.cache_data.clear() 
        gc.collect()
        st.rerun()

    st.markdown("---")
    
    st.subheader("📥 1. Archivo Procesado")
    columnas_exportacion = [col for col in df_analisis.columns if col != 'FECHA_REGISTRO_DT']
    df_para_exportar = df_analisis[columnas_exportacion]
    
    with st.spinner("Preparando archivos procesados para descarga..."):
        csv_data = convert_df_to_csv(df_para_exportar)
        excel_data = convert_df_to_excel(df_para_exportar)
    
    # --- NUEVO: Construimos el nombre dinámico ---
    nombre_base = st.session_state.get('nombre_original', 'archivo').rsplit('.', 1)[0]
    nombre_csv = f"resultado_{nombre_base}.csv"
    nombre_excel = f"resultado_{nombre_base}.xlsx"
    
    col_desc1, col_desc2 = st.columns(2)
    with col_desc1:
        st.download_button(
            label="Descargar Dataset (CSV)",
            data=csv_data,
            file_name=nombre_csv,
            mime="text/csv"
        )
    with col_desc2:
        st.download_button(
            label="Descargar Dataset (Excel)",
            data=excel_data,
            file_name=nombre_excel,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    st.markdown("---")

    tab1, tab2 = st.tabs(["📊 Tabla de Promedios", "📈 Gráfica de Evolución de Precios"])

# === PESTAÑA 1: TABLA ===
    with tab1:
        with st.form("form_filtros_tabla"):
            col1, col2, col3 = st.columns(3)
            with col1:
                # Preparamos la lista combinada con prefijos para la tabla
                depas_t = ["Dep: " + str(d) for d in df_analisis['NOMDEPA'].dropna().unique()]
                provs_t = ["Prov: " + str(p) for p in df_analisis['NOMPROV'].dropna().unique()]
                dists_t = ["Dist: " + str(d) for d in df_analisis['NOMDIST'].dropna().unique()]
                
                todas_ubicaciones_t = sorted(depas_t) + sorted(provs_t) + sorted(dists_t)
                ubicaciones_sel_t = st.multiselect("Seleccionar Ubicación(es):", todas_ubicaciones_t)
            with col2:
                prods_disp = df_analisis['DESCRIPCION_PRODUCTO'].dropna().unique().tolist()
                prods_sel = st.multiselect("Seleccionar Producto(s):", prods_disp)
            with col3:
                min_date = df_analisis['FECHA_REGISTRO_DT'].min().date()
                max_date = df_analisis['FECHA_REGISTRO_DT'].max().date()
                rango_fechas_tabla = st.date_input("Rango de Fechas:", [min_date, max_date], min_value=min_date, max_value=max_date)

            submit_tabla = st.form_submit_button("📊 Calcular Promedios")

        if submit_tabla:
            if ubicaciones_sel_t and prods_sel and len(rango_fechas_tabla) == 2:
                start_date, end_date = rango_fechas_tabla
                
                with st.spinner("Calculando promedios por ubicación..."):
                    df_tabla_list = []
                    
                    # Iteramos sobre los lugares seleccionados para extraer su data
                    for loc in ubicaciones_sel_t:
                        if loc.startswith("Dep: "):
                            temp_df = df_analisis[df_analisis['NOMDEPA'] == loc.replace("Dep: ", "")].copy()
                        elif loc.startswith("Prov: "):
                            temp_df = df_analisis[df_analisis['NOMPROV'] == loc.replace("Prov: ", "")].copy()
                        elif loc.startswith("Dist: "):
                            temp_df = df_analisis[df_analisis['NOMDIST'] == loc.replace("Dist: ", "")].copy()
                        
                        if not temp_df.empty:
                            temp_df['UBICACION'] = loc
                            df_tabla_list.append(temp_df)
                            
                    if df_tabla_list:
                        # Unimos todos los sub-dataframes
                        df_filtrado_tabla = pd.concat(df_tabla_list, ignore_index=True)
                        
                        # Filtramos por producto y fecha
                        mask_tabla = (
                            (df_filtrado_tabla['DESCRIPCION_PRODUCTO'].isin(prods_sel)) &
                            (df_filtrado_tabla['FECHA_REGISTRO_DT'].dt.date >= start_date) &
                            (df_filtrado_tabla['FECHA_REGISTRO_DT'].dt.date <= end_date)
                        )
                        df_filtrado_tabla = df_filtrado_tabla[mask_tabla]
                        
                        if not df_filtrado_tabla.empty:
                            # Agrupamos por Día, Ubicación (dinámica) y Producto
                            df_promedio = df_filtrado_tabla.groupby(
                                ['FECHA_REGISTRO', 'FECHA_REGISTRO_DT', 'UBICACION', 'DESCRIPCION_PRODUCTO'], 
                                observed=True
                            ).agg(
                                PRECIO_PROMEDIO=('PRECIO_VENTA', 'mean')
                            ).reset_index()
                            
                            # Ordenamos cronológicamente y eliminamos datetime
                            df_promedio = df_promedio.sort_values('FECHA_REGISTRO_DT').drop(columns=['FECHA_REGISTRO_DT'])
                            
                            # Reordenamos las columnas
                            df_promedio = df_promedio[['FECHA_REGISTRO', 'UBICACION', 'DESCRIPCION_PRODUCTO', 'PRECIO_PROMEDIO']]
                            
                            # Guardamos en sesión
                            st.session_state['df_promedio'] = df_promedio
                            st.session_state['csv_promedio'] = convert_df_to_csv(df_promedio)
                            st.session_state['excel_promedio'] = convert_df_to_excel(df_promedio)
                        else:
                            st.session_state['df_promedio'] = None
                            st.info("No hay datos para los productos y fechas seleccionados en esas ubicaciones.")
                    else:
                        st.session_state['df_promedio'] = None
                        st.info("No se encontraron registros para las ubicaciones seleccionadas.")
            else:
                st.warning("⚠️ Por favor, selecciona al menos una ubicación, un producto y verifica el rango de fechas.")

        # Renderizado visual y botones de descarga
        if 'df_promedio' in st.session_state and st.session_state['df_promedio'] is not None:
            st.dataframe(st.session_state['df_promedio'], use_container_width=True)
            
            st.markdown("#### 📥 Descargar Tabla de Promedios")
            col_dl1, col_dl2 = st.columns(2)
            with col_dl1:
                st.download_button(
                    label="Descargar Promedios (CSV)",
                    data=st.session_state['csv_promedio'], 
                    file_name="tabla_promedios_ubicaciones.csv",
                    mime="text/csv",
                    key="dl_prom_csv"
                )
            with col_dl2:
                st.download_button(
                    label="Descargar Promedios (Excel)",
                    data=st.session_state['excel_promedio'], 
                    file_name="tabla_promedios_ubicaciones.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_prom_excel"
                )

# === PESTAÑA 2: GRÁFICA ===
    with tab2:
        with st.form("form_filtros_grafica"):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                # 1. Preparamos la lista combinada de todas las ubicaciones con prefijos
                depas = ["Dep: " + str(d) for d in df_analisis['NOMDEPA'].dropna().unique()]
                provs = ["Prov: " + str(p) for p in df_analisis['NOMPROV'].dropna().unique()]
                dists = ["Dist: " + str(d) for d in df_analisis['NOMDIST'].dropna().unique()]
                
                todas_ubicaciones = sorted(depas) + sorted(provs) + sorted(dists)
                ubicaciones_sel = st.multiselect("Lugares a comparar:", todas_ubicaciones)
                
            with col2:
                prods_disp_graf = df_analisis['DESCRIPCION_PRODUCTO'].dropna().unique().tolist()
                prods_sel_graf = st.multiselect("Producto(s):", prods_disp_graf)
            with col3:
                rango_fechas_graf = st.date_input("Fechas del gráfico:", [min_date, max_date], min_value=min_date, max_value=max_date, key="fechas_graf")
            with col4:
                agrupacion = st.selectbox("Visualizar por:", ["Día", "Mes", "Trimestre", "Semestre", "Año"])
                mapa_freq = {"Día": "D", "Mes": "ME", "Trimestre": "QE", "Semestre": "6ME", "Año": "YE"}

            submit_grafica = st.form_submit_button("📈 Generar Gráfica")

        if submit_grafica:
            if ubicaciones_sel and prods_sel_graf and len(rango_fechas_graf) == 2:
                start_date_g, end_date_g = rango_fechas_graf
                freq = mapa_freq[agrupacion]
                
                with st.spinner("Analizando tendencias comparativas..."):
                    df_plot_list = []
                    
                    # 2. Iteramos sobre los lugares seleccionados para extraer y etiquetar su data
                    for loc in ubicaciones_sel:
                        if loc.startswith("Dep: "):
                            temp_df = df_analisis[df_analisis['NOMDEPA'] == loc.replace("Dep: ", "")].copy()
                        elif loc.startswith("Prov: "):
                            temp_df = df_analisis[df_analisis['NOMPROV'] == loc.replace("Prov: ", "")].copy()
                        elif loc.startswith("Dist: "):
                            temp_df = df_analisis[df_analisis['NOMDIST'] == loc.replace("Dist: ", "")].copy()
                        
                        if not temp_df.empty:
                            temp_df['LUGAR_COMPARACION'] = loc # Etiqueta clave para la gráfica
                            df_plot_list.append(temp_df)
                    
                    if df_plot_list:
                        # Unimos todos los sub-dataframes
                        df_filtrado_graf = pd.concat(df_plot_list, ignore_index=True)
                        
                        # Filtramos por los productos y fechas seleccionados
                        mask_graf = (
                            (df_filtrado_graf['DESCRIPCION_PRODUCTO'].isin(prods_sel_graf)) &
                            (df_filtrado_graf['FECHA_REGISTRO_DT'].dt.date >= start_date_g) &
                            (df_filtrado_graf['FECHA_REGISTRO_DT'].dt.date <= end_date_g)
                        )
                        df_filtrado_graf = df_filtrado_graf[mask_graf]
                        
                        if not df_filtrado_graf.empty:
                            df_filtrado_graf.set_index('FECHA_REGISTRO_DT', inplace=True)
                            
                            # 3. Agrupamos por el nuevo Lugar y Producto
                            df_resampled = df_filtrado_graf.groupby(['LUGAR_COMPARACION', 'DESCRIPCION_PRODUCTO'], observed=True)['PRECIO_VENTA'].resample(freq).mean().reset_index()
                            df_resampled.dropna(subset=['PRECIO_VENTA'], inplace=True)
                            
                            if agrupacion != "Día":
                                df_resampled['Periodo'] = df_resampled['FECHA_REGISTRO_DT'].dt.strftime('%Y-%m-%d')
                            else:
                                df_resampled['Periodo'] = df_resampled['FECHA_REGISTRO_DT']

                            # 4. Creamos una leyenda única para que Plotly dibuje líneas separadas
                            df_resampled['LEYENDA'] = df_resampled['LUGAR_COMPARACION'] + " | " + df_resampled['DESCRIPCION_PRODUCTO'].astype(str)

                            fig = px.line(
                                df_resampled, x='Periodo', y='PRECIO_VENTA', color='LEYENDA',
                                markers=True, title="Evolución Comparativa de Precios por Ubicación"
                            )
                            st.session_state['figura_grafica'] = fig
                        else:
                            st.session_state['figura_grafica'] = None
                            st.info("No hay datos para graficar en las fechas y productos seleccionados.")
                    else:
                        st.session_state['figura_grafica'] = None
                        st.info("No se encontraron registros para las ubicaciones seleccionadas.")
            else:
                st.warning("⚠️ Por favor, selecciona al menos un lugar, un producto y verifica las fechas.")

        # --- RENDERIZADO VISUAL INMEDIATO ---
        if 'figura_grafica' in st.session_state and st.session_state['figura_grafica'] is not None:
            st.plotly_chart(st.session_state['figura_grafica'], use_container_width=True)