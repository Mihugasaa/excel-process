import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px

# Configuración inicial de la página
st.set_page_config(page_title="Dashboard Analítico de Precios", layout="wide")

st.title("Procesador y Dashboard de Precios")
st.write("Sube tu archivo para estructurar los datos, rellenar vacíos temporales y explorar el comportamiento de los precios.")

# Inicializar variables en session_state para mantener los datos al usar filtros
if 'df_final' not in st.session_state:
    st.session_state['df_final'] = None

# Función para convertir dataframe a Excel en memoria para descargas
def convert_df_to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    return output.getvalue()

def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

# 1. Subida de archivo
uploaded_file = st.file_uploader("Selecciona tu documento Excel", type=['xlsx', 'xls'])

if uploaded_file is not None and st.session_state['df_final'] is None:
    col1, col2 = st.columns(2)
    iniciar = col1.button("▶️ Procesar Archivo", type="primary")
    cancelar = col2.button("⏹️ Cancelar / Detener")

    if cancelar:
        st.warning("Proceso detenido por el usuario.")
        st.stop()

    if iniciar:
        progress_bar = st.progress(0)
        status_text = st.empty()

        try:
            # --- FASE 1: Lectura ---
            status_text.text("Fase 1/5: Leyendo el archivo Excel...")
            df = pd.read_excel(uploaded_file)
            columnas_originales = df.columns.tolist()
            progress_bar.progress(20)

            # --- FASE 2: Optimización y Limpieza ---
            status_text.text("Fase 2/5: Optimizando memoria y preparando datos...")
            columnas_texto = [
                'DESCRIPCION_ACTIVIDAD', 'CODIGO', 'CODIGO_OSINERG', 'NMBRE_UNDAD',
                'RUC', 'NOMDEPA', 'NOMPROV', 'NOMDIST', 'DIRECCION', 'DESCRIPCION_PRODUCTO', 'NRO_RGSTRO'
            ]
            for col in columnas_texto:
                if col in df.columns:
                    df[col] = df[col].astype('category')
            
            # Asegurarse de que el precio sea numérico
            if 'PRECIO_VENTA' in df.columns:
                df['PRECIO_VENTA'] = pd.to_numeric(df['PRECIO_VENTA'], errors='coerce')
            
            df['FECHA_REGISTRO_DT'] = pd.to_datetime(df['FECHA_REGISTRO'], format='%d/%m/%Y', errors='coerce')
            df['orden_original'] = np.arange(len(df), dtype=np.float64)
            progress_bar.progress(40)

            # --- FASE 3: Cálculo matemático y saltos ---
            status_text.text("Fase 3/5: Calculando fechas y evaluando saltos temporales...")
            columnas_sort = ['CODIGO_OSINERG', 'DESCRIPCION_PRODUCTO', 'FECHA_REGISTRO_DT', 'orden_original']
            df_sorted = df.sort_values(by=columnas_sort)

            df_sorted['next_date'] = df_sorted.groupby(['CODIGO_OSINERG', 'DESCRIPCION_PRODUCTO'], observed=True)['FECHA_REGISTRO_DT'].shift(-1)
            df_sorted['days_diff'] = (df_sorted['next_date'] - df_sorted['FECHA_REGISTRO_DT']).dt.days

            gaps = df_sorted[df_sorted['days_diff'] > 1].copy()
            progress_bar.progress(60)

            # --- FASE 4: Interpolación (Regla de Máximo 15 días) ---
            status_text.text("Fase 4/5: Generando días faltantes (Máx 15 días por salto)...")
            if not gaps.empty:
                # Limitamos la creación de nuevas filas a un máximo de 15 días
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
            else:
                df_final = df.copy()

            df_final = df_final.sort_values(
                by=columnas_sort, 
                ascending=[True, True, False, False]
            ).reset_index(drop=True)
            
            # Limpiamos columnas auxiliares pero MANTENEMOS FECHA_REGISTRO_DT para el análisis posterior
            df_final = df_final.drop(columns=['orden_original'], errors='ignore')
            
            # Guardamos en sesión
            st.session_state['df_final'] = df_final
            
            progress_bar.progress(100)
            status_text.success("¡Procesamiento finalizado con éxito!")
            st.rerun()

        except Exception as e:
            status_text.error(f"Hubo un error al procesar: {e}")
            progress_bar.empty()

# --- MÓDULO DE ANÁLISIS Y DESCARGAS ---
if st.session_state['df_final'] is not None:
    df_analisis = st.session_state['df_final'].copy()
    
    st.success("Datos listos para exploración.")
    
    # Botón para reiniciar/subir un nuevo archivo
    if st.button("🔄 Subir nuevo archivo"):
        st.session_state['df_final'] = None
        st.rerun()

    st.markdown("---")
    
    # Descarga del archivo maestro procesado
    st.subheader("📥 1. Archivo Maestro Procesado")
    columnas_exportacion = [col for col in df_analisis.columns if col != 'FECHA_REGISTRO_DT']
    df_export = df_analisis[columnas_exportacion]
    
    st.download_button(
        label="Descargar Dataset Completo (Excel)",
        data=convert_df_to_excel(df_export),
        file_name="dataset_procesado_interpolado.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.markdown("---")

    # Contenedores de pestañas para organizar el dashboard
    tab1, tab2 = st.tabs(["📊 Tabla de Promedios (Por Grifo)", "📈 Gráfica de Evolución (Por Departamento)"])

    # === PESTAÑA 1: TABLA DE PROMEDIOS ===
    with tab1:
        st.subheader("Promedio Diario de Precios")
        st.write("Selecciona los filtros para visualizar el precio promedio por grifo y producto en un rango de tiempo.")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            grifos_disp = sorted(df_analisis['CODIGO_OSINERG'].dropna().unique())
            grifos_sel = st.multiselect("Seleccionar Grifo (CODIGO_OSINERG):", grifos_disp)
        with col2:
            prods_disp = sorted(df_analisis['DESCRIPCION_PRODUCTO'].dropna().unique())
            prods_sel = st.multiselect("Seleccionar Producto(s):", prods_disp)
        with col3:
            min_date = df_analisis['FECHA_REGISTRO_DT'].min().date()
            max_date = df_analisis['FECHA_REGISTRO_DT'].max().date()
            rango_fechas_tabla = st.date_input("Rango de Fechas:", [min_date, max_date], min_value=min_date, max_value=max_date)

        if grifos_sel and prods_sel and len(rango_fechas_tabla) == 2:
            start_date, end_date = rango_fechas_tabla
            
            # Filtramos el dataframe
            mask_tabla = (
                (df_analisis['CODIGO_OSINERG'].isin(grifos_sel)) & 
                (df_analisis['DESCRIPCION_PRODUCTO'].isin(prods_sel)) &
                (df_analisis['FECHA_REGISTRO_DT'].dt.date >= start_date) &
                (df_analisis['FECHA_REGISTRO_DT'].dt.date <= end_date)
            )
            df_filtrado_tabla = df_analisis[mask_tabla]
            
            if not df_filtrado_tabla.empty:
                # Agrupación y cálculo del promedio
                df_promedio = df_filtrado_tabla.groupby(
                    ['FECHA_REGISTRO', 'FECHA_REGISTRO_DT', 'CODIGO_OSINERG', 'DESCRIPCION_PRODUCTO'], 
                    observed=True
                )['PRECIO_VENTA'].mean().reset_index()
                
                # Ordenamos cronológicamente para mostrar
                df_promedio = df_promedio.sort_values('FECHA_REGISTRO_DT').drop(columns=['FECHA_REGISTRO_DT'])
                df_promedio.rename(columns={'PRECIO_VENTA': 'PRECIO_PROMEDIO'}, inplace=True)
                
                st.dataframe(df_promedio, use_container_width=True)
                
                # Botón de descarga de esta tabla
                st.download_button(
                    label="📥 Descargar Tabla de Promedios (CSV)",
                    data=convert_df_to_csv(df_promedio),
                    file_name="promedios_grifo_producto.csv",
                    mime="text/csv"
                )
            else:
                st.info("No hay datos para los filtros seleccionados.")

    # === PESTAÑA 2: GRÁFICA DE EVOLUCIÓN ===
    with tab2:
        st.subheader("Evolución de Precios en el Tiempo")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            depas_disp = sorted(df_analisis['NOMDEPA'].dropna().unique())
            depa_sel = st.selectbox("Seleccionar Departamento:", [""] + list(depas_disp))
        with col2:
            prods_disp_graf = sorted(df_analisis['DESCRIPCION_PRODUCTO'].dropna().unique())
            prods_sel_graf = st.multiselect("Producto(s) a comparar:", prods_disp_graf)
        with col3:
            rango_fechas_graf = st.date_input("Fechas del gráfico:", [min_date, max_date], min_value=min_date, max_value=max_date, key="fechas_graf")
        with col4:
            agrupacion = st.selectbox("Visualizar por:", ["Día", "Mes", "Trimestre", "Semestre", "Año"])
            
            # Diccionario para mapear la selección con la lógica de resample de pandas
            mapa_freq = {
                "Día": "D",
                "Mes": "ME",
                "Trimestre": "QE",
                "Semestre": "6ME",
                "Año": "YE"
            }

        if depa_sel and prods_sel_graf and len(rango_fechas_graf) == 2:
            start_date_g, end_date_g = rango_fechas_graf
            freq = mapa_freq[agrupacion]
            
            mask_graf = (
                (df_analisis['NOMDEPA'] == depa_sel) & 
                (df_analisis['DESCRIPCION_PRODUCTO'].isin(prods_sel_graf)) &
                (df_analisis['FECHA_REGISTRO_DT'].dt.date >= start_date_g) &
                (df_analisis['FECHA_REGISTRO_DT'].dt.date <= end_date_g)
            )
            df_filtrado_graf = df_analisis[mask_graf].copy()
            
            if not df_filtrado_graf.empty:
                # Preparamos el resample: necesitamos que la fecha sea el índice
                df_filtrado_graf.set_index('FECHA_REGISTRO_DT', inplace=True)
                
                # Agrupamos por producto y la frecuencia temporal seleccionada
                df_resampled = df_filtrado_graf.groupby(['DESCRIPCION_PRODUCTO'], observed=True)['PRECIO_VENTA'].resample(freq).mean().reset_index()
                df_resampled.dropna(subset=['PRECIO_VENTA'], inplace=True) # Quitamos vacíos por si la agrupación genera nulos
                
                # Formateamos un poco la fecha visualmente si no es día
                if agrupacion != "Día":
                    df_resampled['Periodo'] = df_resampled['FECHA_REGISTRO_DT'].dt.strftime('%Y-%m-%d')
                else:
                    df_resampled['Periodo'] = df_resampled['FECHA_REGISTRO_DT']

                # Creación de la gráfica con Plotly
                fig = px.line(
                    df_resampled, 
                    x='Periodo', 
                    y='PRECIO_VENTA', 
                    color='DESCRIPCION_PRODUCTO',
                    markers=True,
                    title=f"Evolución del Precio Promedio en {depa_sel} (Agrupado por {agrupacion})",
                    labels={'PRECIO_VENTA': 'Precio Promedio', 'Periodo': 'Tiempo', 'DESCRIPCION_PRODUCTO': 'Producto'}
                )
                
                st.plotly_chart(fig, use_container_width=True)
                st.caption("💡 Tip: Puedes descargar esta gráfica como imagen usando el ícono de la cámara 📷 que aparece en la esquina superior derecha del gráfico al pasar el ratón.")
                
                # Botón de descarga de los datos de la gráfica
                st.download_button(
                    label="📥 Descargar Datos del Gráfico (CSV)",
                    data=convert_df_to_csv(df_resampled),
                    file_name=f"evolucion_precios_{depa_sel}_{agrupacion.lower()}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No hay datos para graficar con los parámetros seleccionados.")