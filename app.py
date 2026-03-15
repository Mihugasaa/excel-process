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

# --- HELPERS (Software Engineering) ---
@st.cache_data(show_spinner=False)
def convert_df_to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    return output.getvalue()

@st.cache_data(show_spinner=False)
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

def filtrar_por_ubicacion(df, lista_ubicaciones):
    """Optimización (Data Science): Filtra vectorialmente en lugar de hacer múltiples copies en un for-loop."""
    depas = [loc.replace("Dep: ", "") for loc in lista_ubicaciones if loc.startswith("Dep: ")]
    provs = [loc.replace("Prov: ", "") for loc in lista_ubicaciones if loc.startswith("Prov: ")]
    dists = [loc.replace("Dist: ", "") for loc in lista_ubicaciones if loc.startswith("Dist: ")]
    
    dfs_filtrados = []
    if depas:
        temp = df[df['NOMDEPA'].isin(depas)].copy()
        temp['UBICACION'] = "Dep: " + temp['NOMDEPA'].astype(str)
        dfs_filtrados.append(temp)
    if provs:
        temp = df[df['NOMPROV'].isin(provs)].copy()
        temp['UBICACION'] = "Prov: " + temp['NOMPROV'].astype(str)
        dfs_filtrados.append(temp)
    if dists:
        temp = df[df['NOMDIST'].isin(dists)].copy()
        temp['UBICACION'] = "Dist: " + temp['NOMDIST'].astype(str)
        dfs_filtrados.append(temp)
        
    if dfs_filtrados:
        return pd.concat(dfs_filtrados, ignore_index=True)
    return pd.DataFrame()

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
    
    # 1. Convertimos a datetime de forma flexible indicando que el día va primero (dayfirst=True)
    # 2. Usamos .dt.normalize() para eliminar las horas, minutos y segundos de raíz
    df['FECHA_REGISTRO_DT'] = pd.to_datetime(df['FECHA_REGISTRO'], dayfirst=True, errors='coerce').dt.normalize()
    
    # 3. Sobrescribimos la columna original para que visualmente también pierda la hora
    # Esto asegura que el groupby() agrupe todo el día correctamente
    df['FECHA_REGISTRO'] = df['FECHA_REGISTRO_DT'].dt.strftime('%d/%m/%Y')
    
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
    progress_bar.progress(75)

    status_text.text("Fase 5/5: Ordenando y limpiando la estructura final...")
    df_final = df_final.sort_values(by=columnas_sort, ascending=[True, True, False, False]).reset_index(drop=True)
    df_final = df_final.drop(columns=['orden_original'], errors='ignore')
    
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
                df_procesado = procesar_archivo(uploaded_file, uploaded_file.name, progress_bar, status_text)
                st.session_state['df_final'] = df_procesado
                st.session_state['nombre_original'] = uploaded_file.name 
                
                status_text.text("Fase Extra: Comprimiendo archivos para descarga (Esto tomará unos segundos)...")
                progress_bar.progress(85)
                
                columnas_exportacion = [col for col in df_procesado.columns if col != 'FECHA_REGISTRO_DT']
                df_para_exportar = df_procesado[columnas_exportacion]
                
                st.session_state['csv_maestro'] = convert_df_to_csv(df_para_exportar)
                progress_bar.progress(90)
                
                st.session_state['excel_maestro'] = convert_df_to_excel(df_para_exportar)
                progress_bar.progress(100)
                
                st.session_state['procesado'] = True
                status_text.success("¡Datos procesados y listos para usar!")
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
        st.session_state.clear() 
        st.cache_data.clear() 
        gc.collect()
        st.rerun()

    st.markdown("---")
    
    st.subheader("📥 1. Archivo Procesado")
    nombre_base = st.session_state.get('nombre_original', 'archivo').rsplit('.', 1)[0]
    
    col_desc1, col_desc2 = st.columns(2)
    with col_desc1:
        st.download_button(
            label="Descargar Dataset (CSV)",
            data=st.session_state['csv_maestro'],
            file_name=f"resultado_{nombre_base}.csv",
            mime="text/csv"
        )
    with col_desc2:
        st.download_button(
            label="Descargar Dataset (Excel)",
            data=st.session_state['excel_maestro'],
            file_name=f"resultado_{nombre_base}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    st.markdown("---")
    st.subheader("📊 2. Módulo de Análisis")
    tab1, tab2 = st.tabs(["Tabla de Promedios", "Gráfica de Evolución de Precios"])

    # === PESTAÑA 1: TABLA ===
    with tab1:
        with st.form("form_filtros_tabla"):
            st.markdown("**A. Selecciona las ubicaciones:**")
            col_loc1, col_loc2, col_loc3 = st.columns(3)
            with col_loc1:
                depas_sel_t = st.multiselect("Departamentos:", sorted(df_analisis['NOMDEPA'].dropna().unique()))
            with col_loc2:
                provs_sel_t = st.multiselect("Provincias:", sorted(df_analisis['NOMPROV'].dropna().unique()))
            with col_loc3:
                dists_sel_t = st.multiselect("Distritos:", sorted(df_analisis['NOMDIST'].dropna().unique()))

            st.markdown("**B. Selecciona parámetros del producto:**")
            col_param1, col_param2 = st.columns(2)
            with col_param1:
                prods_sel = st.multiselect("Producto(s):", df_analisis['DESCRIPCION_PRODUCTO'].dropna().unique().tolist())
            with col_param2:
                min_date = df_analisis['FECHA_REGISTRO_DT'].min().date()
                max_date = df_analisis['FECHA_REGISTRO_DT'].max().date()
                rango_fechas_tabla = st.date_input("Rango de Fechas:", [min_date, max_date], min_value=min_date, max_value=max_date, key="fechas_t")

            submit_tabla = st.form_submit_button("Calcular Promedios")

        if submit_tabla:
            ubicaciones_sel_t = ["Dep: " + d for d in depas_sel_t] + ["Prov: " + p for p in provs_sel_t] + ["Dist: " + d for d in dists_sel_t]
            
            if ubicaciones_sel_t and prods_sel and len(rango_fechas_tabla) == 2:
                start_date, end_date = rango_fechas_tabla
                
                with st.spinner("Calculando promedios por ubicación..."):
                    # Uso de la función refactorizada para mayor velocidad
                    df_filtrado_tabla = filtrar_por_ubicacion(df_analisis, ubicaciones_sel_t)
                    
                    if not df_filtrado_tabla.empty:
                        mask_tabla = (
                            (df_filtrado_tabla['DESCRIPCION_PRODUCTO'].isin(prods_sel)) &
                            (df_filtrado_tabla['FECHA_REGISTRO_DT'].dt.date >= start_date) &
                            (df_filtrado_tabla['FECHA_REGISTRO_DT'].dt.date <= end_date)
                        )
                        df_filtrado_tabla = df_filtrado_tabla[mask_tabla]
                        
                        if not df_filtrado_tabla.empty:
                            df_promedio = df_filtrado_tabla.groupby(
                                ['FECHA_REGISTRO', 'FECHA_REGISTRO_DT', 'UBICACION', 'DESCRIPCION_PRODUCTO'], 
                                observed=True
                            ).agg(PRECIO_PROMEDIO=('PRECIO_VENTA', 'mean')).reset_index()
                            
                            df_promedio = df_promedio.sort_values('FECHA_REGISTRO_DT').drop(columns=['FECHA_REGISTRO_DT'])
                            df_promedio = df_promedio[['FECHA_REGISTRO', 'UBICACION', 'DESCRIPCION_PRODUCTO', 'PRECIO_PROMEDIO']]
                            
                            st.session_state['df_promedio'] = df_promedio
                            st.session_state['csv_promedio'] = convert_df_to_csv(df_promedio)
                            st.session_state['excel_promedio'] = convert_df_to_excel(df_promedio)
                        else:
                            st.session_state['df_promedio'] = None
                            st.info("No hay datos para los productos y fechas seleccionados.")
                    else:
                        st.session_state['df_promedio'] = None
                        st.info("No se encontraron registros para las ubicaciones.")
            else:
                st.warning("⚠️ Selecciona al menos una ubicación, un producto y verifica las fechas.")

        # UX MEJORA: Elementos relacionados juntos visualmente
        if 'df_promedio' in st.session_state and st.session_state['df_promedio'] is not None:
            st.dataframe(st.session_state['df_promedio'], use_container_width=True)
            
            col_dl1, col_dl2 = st.columns(2)
            with col_dl1:
                st.download_button("📥 Descargar Tabla (CSV)", st.session_state['csv_promedio'], "promedios.csv", "text/csv", key="dl_prom_csv")
            with col_dl2:
                st.download_button("📥 Descargar Tabla (Excel)", st.session_state['excel_promedio'], "promedios.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_prom_excel")

    # === PESTAÑA 2: GRÁFICA ===
    with tab2:
        with st.form("form_filtros_grafica"):
            st.markdown("**A. Selecciona las ubicaciones a comparar:**")
            col_loc1, col_loc2, col_loc3 = st.columns(3)
            with col_loc1:
                depas_sel_g = st.multiselect("Departamentos:", sorted(df_analisis['NOMDEPA'].dropna().unique()), key="dep_g")
            with col_loc2:
                provs_sel_g = st.multiselect("Provincias:", sorted(df_analisis['NOMPROV'].dropna().unique()), key="prov_g")
            with col_loc3:
                dists_sel_g = st.multiselect("Distritos:", sorted(df_analisis['NOMDIST'].dropna().unique()), key="dist_g")
                
            st.markdown("**B. Selecciona parámetros de visualización:**")
            col_param1, col_param2, col_param3 = st.columns(3)
            with col_param1:
                prods_sel_graf = st.multiselect("Producto(s):", df_analisis['DESCRIPCION_PRODUCTO'].dropna().unique().tolist(), key="prod_g")
            with col_param2:
                rango_fechas_graf = st.date_input("Fechas:", [min_date, max_date], min_value=min_date, max_value=max_date, key="fechas_graf")
            with col_param3:
                agrupacion = st.selectbox("Visualizar por:", ["Día", "Mes", "Trimestre", "Semestre", "Año"])
                mapa_freq = {"Día": "D", "Mes": "ME", "Trimestre": "QE", "Semestre": "6ME", "Año": "YE"}

            submit_grafica = st.form_submit_button("Generar Gráfica")

        if submit_grafica:
            ubicaciones_sel_g = ["Dep: " + d for d in depas_sel_g] + ["Prov: " + p for p in provs_sel_g] + ["Dist: " + d for d in dists_sel_g]
            
            if ubicaciones_sel_g and prods_sel_graf and len(rango_fechas_graf) == 2:
                start_date_g, end_date_g = rango_fechas_graf
                freq = mapa_freq[agrupacion]
                
                with st.spinner("Analizando tendencias comparativas..."):
                    # Uso de la función refactorizada
                    df_filtrado_graf = filtrar_por_ubicacion(df_analisis, ubicaciones_sel_g)
                    
                    if not df_filtrado_graf.empty:
                        mask_graf = (
                            (df_filtrado_graf['DESCRIPCION_PRODUCTO'].isin(prods_sel_graf)) &
                            (df_filtrado_graf['FECHA_REGISTRO_DT'].dt.date >= start_date_g) &
                            (df_filtrado_graf['FECHA_REGISTRO_DT'].dt.date <= end_date_g)
                        )
                        df_filtrado_graf = df_filtrado_graf[mask_graf]
                        
                        if not df_filtrado_graf.empty:
                            df_filtrado_graf.set_index('FECHA_REGISTRO_DT', inplace=True)
                            
                            df_resampled = df_filtrado_graf.groupby(['UBICACION', 'DESCRIPCION_PRODUCTO'], observed=True)['PRECIO_VENTA'].resample(freq).mean().reset_index()
                            df_resampled.dropna(subset=['PRECIO_VENTA'], inplace=True)
                            
                            if agrupacion != "Día":
                                df_resampled['Periodo'] = df_resampled['FECHA_REGISTRO_DT'].dt.strftime('%Y-%m-%d')
                            else:
                                df_resampled['Periodo'] = df_resampled['FECHA_REGISTRO_DT']

                            df_resampled['LEYENDA'] = df_resampled['UBICACION'] + " | " + df_resampled['DESCRIPCION_PRODUCTO'].astype(str)
                            df_resampled['PRECIO_LABEL'] = df_resampled['PRECIO_VENTA'].round(2).astype(str) # Conversión a string para evitar errores en Plotly

                            fig = px.line(
                                df_resampled, x='Periodo', y='PRECIO_VENTA', color='LEYENDA',
                                markers=True, text='PRECIO_LABEL', title="Evolución Comparativa de Precios"
                            )
                            
                            fig.update_traces(textposition="top center")
                            fig.update_layout(margin=dict(t=50))

                            st.session_state['figura_grafica'] = fig
                        else:
                            st.session_state['figura_grafica'] = None
                            st.info("No hay datos para graficar en las fechas y productos seleccionados.")
                    else:
                        st.session_state['figura_grafica'] = None
                        st.info("No se encontraron registros para las ubicaciones.")
            else:
                st.warning("⚠️ Selecciona al menos un lugar, un producto y verifica las fechas.")

        if 'figura_grafica' in st.session_state and st.session_state['figura_grafica'] is not None:
            st.plotly_chart(st.session_state['figura_grafica'], use_container_width=True)