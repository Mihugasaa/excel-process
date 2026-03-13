import streamlit as st
import pandas as pd
import numpy as np
import io

# 1. Título y diseño de la página web
st.title("Procesador de Datos Excel")
st.write("Sube tu archivo para estructurar los datos y rellenar los días faltantes.")

# 2. Botón para subir el archivo
uploaded_file = st.file_uploader("Selecciona tu documento Excel", type=['xlsx', 'xls'])

if uploaded_file is not None:
    # Usamos columnas para poner los botones de Iniciar y Cancelar uno al lado del otro
    col1, col2 = st.columns(2)
    iniciar = col1.button("▶️ Procesar Archivo", type="primary")
    cancelar = col2.button("⏹️ Cancelar / Detener")

    # Si se presiona cancelar, detenemos el flujo inmediatamente
    if cancelar:
        st.warning("Proceso detenido por el usuario.")
        st.stop()

    if iniciar:
        # Inicializamos la barra de progreso y el texto de estado
        progress_bar = st.progress(0)
        status_text = st.empty()

        try:
            # --- FASE 1: Lectura ---
            status_text.text("Fase 1/5: Leyendo el archivo Excel (Esto suele tardar más)...")
            df = pd.read_excel(uploaded_file)
            columnas_originales = df.columns.tolist()
            progress_bar.progress(20) # 20% completado

            # --- FASE 2: Optimización ---
            status_text.text("Fase 2/5: Optimizando memoria del sistema...")
            columnas_texto = [
                'DESCRIPCION_ACTIVIDAD', 'CODIGO', 'CODIGO_OSINERG', 'NMBRE_UNDAD',
                'RUC', 'NOMDEPA', 'NOMPROV', 'NOMDIST', 'DIRECCION', 'DESCRIPCION_PRODUCTO', 'NRO_RGSTRO'
            ]
            for col in columnas_texto:
                if col in df.columns:
                    df[col] = df[col].astype('category')
            
            df['FECHA_REGISTRO_DT'] = pd.to_datetime(df['FECHA_REGISTRO'], format='%d/%m/%Y', errors='coerce')
            df['orden_original'] = np.arange(len(df), dtype=np.float64)
            progress_bar.progress(40) # 40% completado

            # --- FASE 3: Cálculo matemático ---
            status_text.text("Fase 3/5: Calculando fechas y saltos temporales...")
            columnas_sort = ['CODIGO_OSINERG', 'DESCRIPCION_PRODUCTO', 'FECHA_REGISTRO_DT', 'orden_original']
            df_sorted = df.sort_values(by=columnas_sort)

            df_sorted['next_date'] = df_sorted.groupby(['CODIGO_OSINERG', 'DESCRIPCION_PRODUCTO'], observed=True)['FECHA_REGISTRO_DT'].shift(-1)
            df_sorted['days_diff'] = (df_sorted['next_date'] - df_sorted['FECHA_REGISTRO_DT']).dt.days

            gaps = df_sorted[df_sorted['days_diff'] > 1].copy()
            progress_bar.progress(60) # 60% completado

            # --- FASE 4: Interpolación ---
            status_text.text("Fase 4/5: Generando e insertando los días faltantes...")
            if not gaps.empty:
                gaps['num_new_rows'] = (gaps['days_diff'] - 1).astype(np.int32)
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
            
            df_final = df_final.drop(columns=['orden_original', 'FECHA_REGISTRO_DT'], errors='ignore')
            df_final = df_final[columnas_originales]
            progress_bar.progress(80) # 80% completado

            # --- FASE 5: Exportación ---
            status_text.text("Fase 5/5: Estructurando y empaquetando el archivo final...")
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_final.to_excel(writer, index=False, sheet_name='Data', header=False, startrow=1)
                worksheet = writer.sheets['Data']
                for col_num, col_name in enumerate(df_final.columns):
                    worksheet.write_string(0, col_num, str(col_name))

            progress_bar.progress(100) # 100% completado
            status_text.success("¡Procesamiento finalizado con éxito!")

            # Mostrar el botón de descarga
            st.download_button(
                label="📥 Descargar archivo procesado",
                data=output.getvalue(),
                file_name=f"resultado_{uploaded_file.name}",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        except Exception as e:
            status_text.error(f"Hubo un error al procesar: {e}")
            progress_bar.empty() # Oculta la barra si hay error
