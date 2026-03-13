import streamlit as st
import pandas as pd
import numpy as np
import io

# 1. Título y diseño de la página web
st.title("Procesador de Datos Excel")
st.write("Sube tu archivo para estructurar los datos y rellenar los días faltantes.")

# 2. Botón para subir el archivo (reemplaza a files.upload de Colab)
uploaded_file = st.file_uploader("Selecciona tu documento Excel", type=['xlsx', 'xls'])

# Si el usuario sube un archivo, mostramos el botón de procesar
if uploaded_file is not None:
    if st.button("Procesar Archivo"):
        
        # Muestra una ruedita de carga mientras piensa
        with st.spinner("Procesando datos... Esto puede demorar un poco."):
            try:
                # Leer archivo directamente desde el botón de subida
                df = pd.read_excel(uploaded_file)
                columnas_originales = df.columns.tolist()

                # --- TU LÓGICA ORIGINAL INTACTA ---
                columnas_texto = [
                    'DESCRIPCION_ACTIVIDAD', 'CODIGO', 'CODIGO_OSINERG', 'NMBRE_UNDAD',
                    'RUC', 'NOMDEPA', 'NOMPROV', 'NOMDIST', 'DIRECCION', 'DESCRIPCION_PRODUCTO', 'NRO_RGSTRO'
                ]
                for col in columnas_texto:
                    if col in df.columns:
                        df[col] = df[col].astype('category')

                df['FECHA_REGISTRO_DT'] = pd.to_datetime(df['FECHA_REGISTRO'], format='%d/%m/%Y', errors='coerce')
                df['orden_original'] = np.arange(len(df), dtype=np.float64)

                columnas_sort = ['CODIGO_OSINERG', 'DESCRIPCION_PRODUCTO', 'FECHA_REGISTRO_DT', 'orden_original']
                df_sorted = df.sort_values(by=columnas_sort)

                df_sorted['next_date'] = df_sorted.groupby(['CODIGO_OSINERG', 'DESCRIPCION_PRODUCTO'], observed=True)['FECHA_REGISTRO_DT'].shift(-1)
                df_sorted['days_diff'] = (df_sorted['next_date'] - df_sorted['FECHA_REGISTRO_DT']).dt.days

                gaps = df_sorted[df_sorted['days_diff'] > 1].copy()

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

                df_final = df_final.sort_values(by=columnas_sort).reset_index(drop=True)
                df_final = df_final.drop(columns=['orden_original', 'FECHA_REGISTRO_DT'], errors='ignore')
                df_final = df_final[columnas_originales]
                # --- FIN DE TU LÓGICA ---

                # 3. Preparar el archivo para descargar en la web (en la memoria, no en disco)
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_final.to_excel(writer, index=False, sheet_name='Data', header=False, startrow=1)
                    worksheet = writer.sheets['Data']
                    for col_num, col_name in enumerate(df_final.columns):
                        worksheet.write_string(0, col_num, str(col_name))

                # Avisar que terminó bien
                st.success("¡Procesamiento exitoso!")

                # 4. Botón de descarga
                st.download_button(
                    label="📥 Descargar archivo procesado",
                    data=output.getvalue(),
                    file_name=f"resultado_{uploaded_file.name}",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            except Exception as e:
                # Si algo falla, muestra el error en rojo en la web
                st.error(f"Hubo un error al procesar: {e}")