import pandas as pd
import os
import numpy as np
import datetime

# Obtener la fecha y hora actual
now = datetime.datetime.now()

# Convertir a cadena de texto en el formato deseado
date_time_str = now.strftime("%Y-%m-%d %H.%M.%S Hrs")
# Definir la ruta de la carpeta
carpeta = r'C:\Users\aryan\OneDrive - contraloriape\Alerta AFP - avances\DATA consulta amigable\.csv'  # Asegúrate de cambiar esto por la ruta correcta
out="ouput"
# Listar el contenido de la carpeta
contenido = os.listdir(carpeta)
# Palabras clave a buscar en el campo 'META_NOMBRE'
palabras_clave = ['AFP']

# Códigos a buscar en el campo 'SEC_EJEC'
afp_repro='ouput/AFP REPRO_OFICIO.xlsx'
df_afp_repro = pd.read_excel(afp_repro)
df_afp_repro = df_afp_repro.dropna()

codigos=list(df_afp_repro["COD_UE"].astype(int))
pattern_meta_nombre = '|'.join(palabras_clave)

# Lista para almacenar los DataFrames filtrados
dfs_filtrados = []
"""
for archivo in contenido:
    print(archivo)
    # Obtener el nombre base del archivo
    n_name_archivo = archivo[:4]
    # Construir la ruta completa del archivo
    ruta_archivo = os.path.join(carpeta, archivo)
    # Leer el archivo CSV en un DataFrame
    df = pd.read_csv(ruta_archivo, low_memory=False)

    # Filtrar el DataFrame basado en la presencia de palabras clave en 'META_NOMBRE'
    df_palabras_clave = df[df['META_NOMBRE'].str.contains(pattern_meta_nombre, case=False, na=False)]

    # Filtrar adicionalmente por los códigos en 'SEC_EJEC'
    df_codigos = df[df['SEC_EJEC'].isin(codigos)]

    # Fusionar los resultados sin duplicados
    df_resultado = pd.concat([df_palabras_clave, df_codigos]).drop_duplicates()

    # Guardar el DataFrame resultante en la lista
    dfs_filtrados.append(df_resultado)

# Concatenar todos los DataFrames filtrados
df_final = pd.concat(dfs_filtrados)

# Guardar el DataFrame final en un archivo Excel
ruta_archivo_salida = os.path.join(out, "merged_filtered_AFP.xlsx")
df_final.to_excel(ruta_archivo_salida)
print("Proceso terminado")
"""
########################################################################################################################
"""
#todos los si de afp dcumentos#
for archivo in contenido:
    print(archivo)
    # Obtener el nombre base del archivo
    n_name_archivo = archivo[:4]
    # Construir la ruta completa del archivo
    ruta_archivo = os.path.join(carpeta, archivo)
    # Leer el archivo CSV en un DataFrame
    df = pd.read_csv(ruta_archivo, low_memory=False)
    # Crear el campo _KEY_AFP
    df['_KEY_AFP'] = df['META_NOMBRE'].apply(lambda x: 'SI' if any(palabra in x for palabra in palabras_clave) else 'NO')
    # Filtrar el DataFrame basado en la presencia de palabras clave en 'META_NOMBRE'
    df_filtrado = df[df['_KEY_AFP'] == 'SI']
    # Filtrar adicionalmente por los códigos en 'SEC_EJEC'
    df_filtrado = df_filtrado[df_filtrado['SEC_EJEC'].isin(codigos)]
    # Guardar el DataFrame filtrado en la lista
    dfs_filtrados.append(df_filtrado)

# Concatenar todos los DataFrames filtrados
df_final = pd.concat(dfs_filtrados)

# Guardar el DataFrame final en un archivo Excel
ruta_archivo_salida = os.path.join(out, "merged_filtered_AFP_key.xlsx")
df_final.to_excel(ruta_archivo_salida)
print("Proceso terminado")
########################################################################################################################
"""
#realiza primero el filtro de palabras clave y posterior a ello identifica a los que estan en el doc afp #
for archivo in contenido:
    print(archivo)
    # Obtener el nombre base del archivo
    n_name_archivo = archivo[:4]
    # Construir la ruta completa del archivo
    ruta_archivo = os.path.join(carpeta, archivo)
    # Leer el archivo CSV en un DataFrame
    df = pd.read_csv(ruta_archivo, low_memory=False)
    # Crear el campo _AFP_DOCUMENTO
    df['_AFP_DOCUMENTO'] = df['SEC_EJEC'].apply(lambda x: 'SI' if x in codigos else 'NO')
    # Filtrar el DataFrame basado en la presencia de palabras clave en 'META_NOMBRE'
    df_filtrado = df[df['META_NOMBRE'].str.contains(pattern_meta_nombre, case=False, na=False)]
    # Guardar el DataFrame filtrado en un archivo Excel
    nombre_archivo_salida = f"{n_name_archivo}_filterd_AFP.xlsx"
    ruta_archivo_salida = os.path.join(out, nombre_archivo_salida)
    df_filtrado.to_excel(ruta_archivo_salida)
    print(f"{archivo} proceso terminado")

########################################################################################################################

directorio = r'data' # cambiar esto por la ruta correcta
archivos = [f for f in os.listdir(directorio) if f.endswith('.xlsx')]
archivos = [os.path.join(directorio, f) for f in archivos]  # Esto crea una ruta completa para cada archivo

# Lista para almacenar los DataFrames
lista_df = []

# Leer cada archivo y almacenarlo en la lista
for archivo in archivos:
    print(archivo)
    df_temp = pd.read_excel(archivo)
    lista_df.append(df_temp)

# Concatenar todos los DataFrames en uno solo
df_final = pd.concat(lista_df, ignore_index=True)

new_na_MES={ 'MONTO_DEVENGADO_ENERO': 'ENERO',
            'MONTO_DEVENGADO_FEBRERO': 'FEBRERO',
            'MONTO_DEVENGADO_MARZO': 'MARZO',
            'MONTO_DEVENGADO_ABRIL': 'ABRIL',
            'MONTO_DEVENGADO_MAYO': 'MAYO',
            'MONTO_DEVENGADO_JUNIO': 'JUNIO',
            'MONTO_DEVENGADO_JULIO': 'JULIO',
            'MONTO_DEVENGADO_AGOSTO': 'AGOSTO',
            'MONTO_DEVENGADO_SEPTIEMBRE': 'SEPTIEMBRE',
            'MONTO_DEVENGADO_OCTUBRE': 'OCTUBRE',
            'MONTO_DEVENGADO_NOVIEMBRE': 'NOVIEMBRE',
            'MONTO_DEVENGADO_DICIEMBRE': 'DICIEMBRE',
            }
df_final = df_final.rename(columns=new_na_MES)

########################################################################################################################
campos_no_alterar=list(df_final.columns)
indices_a_eliminar = [60,61,62,63,64,65,66,67,68,69,70,71]


for i in campos_no_alterar: print(i)
# Eliminar múltiples elementos por índice
for index in sorted(indices_a_eliminar, reverse=True):
    del campos_no_alterar[index]


# Utilizar la función melt especificando las columnas a mantener inalteradas
df_melted = df_final.melt(id_vars=campos_no_alterar, var_name='_MES', value_name='_DEVENGADO_MES')
df_melted.to_excel(out+"\\"+"_data_test.xlsx")
########################################################################################################################








df_melted["_MES"].unique()


resultado_PIA_AFP = df_final.groupby('ANO_EJE')[['MONTO_PIA', 'MONTO_PIM', 'MONTO_DEVENGADO_ANUAL']].sum().reset_index()
print(resultado_PIA_AFP)
resultado_PIA_AFP.to_excel(out+"\\"+"PPD_2020-2024_resumen_AFP.xlsx")
########################################################################################################################
resultados = []
for i in contenido:
    archivo= carpeta+"//"+i
    #print(archivo)
    df = pd.read_csv(archivo, low_memory=False)
    #print(archivo)
    res_ppd=df.groupby('ANO_EJE')[['MONTO_PIA', 'MONTO_PIM', 'MONTO_DEVENGADO_ANUAL']].sum().reset_index()
    print(res_ppd)
    resultados.append(res_ppd)

df_final_PPD = pd.concat(resultados, ignore_index=True)


df_final_PPD.to_excel("PPD_2020-2024_resumen.xlsx")

for i in df_final["META_NOMBRE"].unique():
    print(i)

for i in df_final["ESPECIFICA_DET_NOMBRE"].unique():
    print(i)

df_final["concat_ruta"]= (        df_final['TIPO_TRANSACCION'].astype(str) +
                            '.' + df_final['GENERICA'].astype(str) +
                            '.' + df_final['SUBGENERICA'].astype(str) +
                            '.' + df_final['SUBGENERICA_DET'].astype(str) +
                            '.' + df_final['ESPECIFICA'].astype(str) +
                            '.' + df_final['ESPECIFICA_DET'].astype(str))

pares_unicos = df_final[['ANO_EJE','concat_ruta','META_NOMBRE', 'ESPECIFICA_DET','ESPECIFICA_DET_NOMBRE']].drop_duplicates()
pares_unicos.to_excel("_meta_especifica_afp.xlsx")
print(pares_unicos)