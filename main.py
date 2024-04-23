import pandas as pd
import os
import numpy as np
import datetime

# Obtener la fecha y hora actual
now = datetime.datetime.now()

# Convertir a cadena de texto en el formato deseado
date_time_str = now.strftime("%Y-%m-%d %H.%M.%S Hrs")

# Definir la ruta de la carpeta
carpeta1 = r'D:\OneDrive - contraloriape\Alerta AFP - avances\DATA consulta amigable\.csv'  # Asegúrate de cambiar esto por la ruta correcta
carpeta2 = r'C:\Users\20764\OneDrive - contraloriape\Alerta AFP - avances\DATA consulta amigable\.csv'  # Asegúrate de cambiar esto por la ruta correcta
out="ouput"
try:
    contenido = os.listdir(carpeta1)
    carpeta=carpeta1
except:
    # Manejo de la excepción ZeroDivisionError del primer bloque try
    print("PC_trabajo")

try:
    contenido = os.listdir(carpeta2)  # Esto lanzará una excepción ValueError
    carpeta = carpeta2
except:
    # Manejo de la excepción ValueError del segundo bloque try
    print("PC_home")

# Listar el contenido de la carpeta

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

df_melted['_CLASIFICACION'] =  (df_melted['TIPO_TRANSACCION'].astype(str) + "." +
                                df_melted['GENERICA'].astype(str)+ "." +
                                df_melted['SUBGENERICA'].astype(str)+ "." +
                                df_melted['SUBGENERICA_DET'].astype(str)+ "." +
                                df_melted['ESPECIFICA'].astype(str)+ "." +
                                df_melted['ESPECIFICA_DET'].astype(str)
                               )
########################################################################################################################
#varaible de comparacion

df_melted['_COMP_CADENA']= (df_melted['ANO_EJE'].astype(str) +
                                df_melted['_CLASIFICACION'].astype(str)+
                                df_melted['META_NOMBRE'].astype(str)
                               )
#varaible de comparacion2

df_melted['_COMP_CADENA2']= (df_melted['ANO_EJE'].astype(str) +
                                df_melted['_CLASIFICACION'].astype(str)+
                                df_melted['META_NOMBRE'].astype(str)
                               )


########################################################################################################################
# Diccionario de mapeo de clasificación _CALIDAD_RUTA_CLASIFICACION
mapeo_clasificacion = {
"20202.1.1.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Parcial",
"20202.1.1.1.1.2PAGO DE CUOTAS DEL REPRO-AFP":"Parcial",
"20202.1.1.3.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Parcial",
"20202.1.1.3.3.1PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20202.1.1.3.3.4PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20202.1.1.9.1.3PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20202.1.1.9.3.98PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20202.1.3.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"Total",
"20202.1.3.1.1.3PAGO DE CUOTAS DEL REPRO - AFP":"Total",
"20202.1.3.1.1.3PAGO DE DEUDA AFP":"Total",
"20202.1.3.1.1.3DEUDA DE ACOGIMIENTO AL REPRO AFP":"Total",
"20202.1.3.1.1.5PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20202.1.3.1.1.5PAGO DE CUOTAS DEL REPRO - AFP":"Nula",
"20202.1.3.1.1.6PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20202.3.1.5.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20202.3.2.8.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20202.3.2.8.1.2PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20202.3.2.8.1.4PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20202.3.2.8.1.5PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20202.5.2.1.1.99TRANSFERENCIA A LAS AFP POR PAGOS INDEBIDOS DE LOS GOBIERNOS REGIONALES AL SISTEMA NACIONAL DE PENSIONES":"Total",
"20202.5.4.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Por validar",
"20202.5.4.1.2.1PAGO DE CUOTAS DEL REPRO-AFP":"Parcial",
"20202.5.4.1.3.1PAGO DE CUOTAS DEL REPRO-AFP":"Parcial",
"20202.5.4.1.3.1PAGO DE FRACCIONAMIENTO POR DEUDAS A LA SUNAT, AFP U OTROS":"Parcial",
"20202.5.4.1.3.1PAGO DE FRACCIONAMIENTO DE DEUDAS A LA SUNAT, AFP Y OTROS":"Parcial",
"20202.5.4.3.2.1PAGO DE CUOTAS DEL REPRO-AFP":"Parcial",
"20202.5.5.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Total",
"20202.5.5.1.1.1PAGO DE FRACCIONAMIENTO POR DEUDAS A LA SUNAT, AFP U OTROS":"Total",
"20202.5.5.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"Total",
"20202.6.3.2.1.2PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20202.6.3.2.3.1PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20202.6.3.2.3.3PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20202.6.3.2.9.4PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20202.6.6.1.3.99PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20212.1.1.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20212.1.1.9.3.99PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20212.1.3.1.1.2PAGO DE CUOTAS DEL REPRO-AFP":"Total",
"20212.1.3.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"Total",
"20212.1.3.1.1.3PAGO DE CUOTAS DEL REPRO - AFP":"Total",
"20212.1.3.1.1.3PAGO DE CUOTAS POR EL ACOGIMIENTO A REPRO AFP II":"Total",
"20212.1.3.1.1.3PAGO DE DEUDA AFP":"Total",
"20212.1.3.1.1.3DEUDA DE ACOGIMIENTO AL REPRO AFP":"Total",
"20212.1.3.1.1.3PAGO DE CUOTAS DEL SINCERAMIENTO DE DEUDAS POR APORTACIONES A LA AFP":"Total",
"20212.1.3.1.1.5PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20212.1.3.1.1.5PAGO DE CUOTAS DEL REPRO - AFP":"Nula",
"20212.1.3.1.1.6PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20212.3.2.7.11.99PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20212.3.2.8.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20212.3.2.8.1.2PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20212.3.2.8.1.4PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20212.3.2.8.1.5PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20212.5.2.1.1.99TRANSFERENCIA A LAS AFP POR PAGOS INDEBIDOS DE LOS GOBIERNOS REGIONALES AL SISTEMA NACIONAL DE PENSIONES":"Total",
"20212.5.4.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Por validar",
"20212.5.4.1.3.1PAGO DE FRACCIONAMIENTO POR DEUDAS A LA SUNAT, AFP U OTROS":"Parcial",
"20212.5.5.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Total",
"20212.5.5.1.1.1PAGO DE FRACCIONAMIENTO POR DEUDAS A LA SUNAT, AFP U OTROS":"Total",
"20212.5.5.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"Total",
"20212.5.5.1.2.1PAGO DE CUOTAS DEL REPRO-AFP":"Total",
"20222.1.1.1.1.2PAGO DE CUOTAS DEL REPRO-AFP":"Parcial",
"20222.1.1.9.3.99PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20222.1.3.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"Total",
"20222.1.3.1.1.3PAGO DE CUOTAS DEL REPRO - AFP":"Total",
"20222.1.3.1.1.3PAGO DE CUOTAS POR EL ACOGIMIENTO A REPRO AFP II":"Total",
"20222.1.3.1.1.3PAGO DE DEUDA AFP":"Total",
"20222.1.3.1.1.3PAGO DE CUOTAS DEL SINCERAMIENTO DE DEUDAS POR APORTACIONES A LA AFP":"Total",
"20222.1.3.1.1.5PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20222.1.3.1.1.5PAGO DE CUOTAS DEL REPRO - AFP":"Nula",
"20222.3.1.5.1.2PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20222.3.1.99.1.99PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20222.3.2.1.2.1PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20222.3.2.1.2.2PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20222.3.2.7.11.99PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20222.3.2.8.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20222.5.2.1.1.99PAGO DE CUOTAS DEL REPRO-AFP":"Total",
"20222.5.2.1.1.99TRANSFERENCIA A LAS AFP POR PAGOS INDEBIDOS DE LOS GOBIERNOS REGIONALES AL SISTEMA NACIONAL DE PENSIONES":"Total",
"20222.5.4.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Por validar",
"20222.5.4.2.2.1PAGO DE CUOTAS DEL REPRO-AFP":"Parcial",
"20222.5.5.1.3.1PAGO DE CUOTAS DEL REPRO-AFP":"Total",
"20222.6.3.2.3.1PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20232.1.1.1.1.2PAGO DE CUOTAS DEL REPRO-AFP":"Parcial",
"20232.1.1.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20232.1.1.9.3.99PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20232.1.3.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"Total",
"20232.1.3.1.1.3PAGO DE CUOTAS DEL REPRO - AFP":"Total",
"20232.1.3.1.1.3ACOGIMIENTO AL REPRO AFP Y DEUDA CON ESSALUD DE LA MUNCIPALIDAD DISTRITAL DE OLMOS":"Total",
"20232.1.3.1.1.3PAGO DE CUOTAS DEL SINCERAMIENTO DE DEUDAS POR APORTACIONES A LA AFP Y ONP":"Total",
"20232.1.3.1.1.3PAGO DE CUOTAS DEL SINCERAMIENTO DE DEUDAS POR APORTACIONES A LA AFP":"Total",
"20232.1.3.1.1.5PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20232.1.3.1.1.5ACOGIMIENTO AL REPRO AFP Y DEUDA CON ESSALUD DE LA MUNCIPALIDAD DISTRITAL DE OLMOS":"Nula",
"20232.1.3.1.1.5PAGO DE CUOTAS DEL REPRO - AFP":"Nula",
"20232.1.3.1.1.6PAGO DE CUOTAS DEL REPRO - AFP":"Nula",
"20232.3.2.7.11.99PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20232.3.2.9.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20232.5.4.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Por validar",
"20232.5.4.3.3.1PAGO DE CUOTAS DEL REPRO-AFP":"Parcial",
"20232.5.5.1.3.1PAGO DE CUOTAS DEL REPRO - AFP":"Total",
"20232.6.3.2.3.3PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20242.1.1.1.1.2PAGO DE CUOTAS DEL REPRO-AFP":"Parcial",
"20242.1.1.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20242.1.1.9.3.99PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20242.1.3.1.1.13PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20242.1.3.1.1.13ACOGIMIENTO AL REPRO AFP Y DEUDA CON ESSALUD DE LA MUNCIPALIDAD DISTRITAL DE OLMOS":"Nula",
"20242.1.3.1.1.13PAGO DE CUOTAS DEL REPRO - AFP":"Nula",
"20242.1.3.1.1.14PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20242.1.3.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"Total",
"20242.1.3.1.1.3PAGO DE CUOTAS DEL REPRO - AFP":"Total",
"20242.1.3.1.1.3ACOGIMIENTO AL REPRO AFP Y DEUDA CON ESSALUD DE LA MUNCIPALIDAD DISTRITAL DE OLMOS":"Total",
"20242.1.3.1.1.3PAGO DE CUOTAS DEL SINCERAMIENTO DE DEUDAS POR APORTACIONES A LA AFP Y ONP":"Total",
"20242.1.3.1.1.3PAGO DE CUOTAS DEL SINCERAMIENTO DE DEUDAS POR APORTACIONES A LA AFP":"Total",
"20242.3.2.7.11.99PAGO DE CUOTAS DEL REPRO-AFP":"Nula",
"20242.5.4.1.1.1SINCERAMIENTO DE DEUDAS ESSALUD Y ONP Y REPRO-AFP":"Por validar",
"20242.5.4.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Por validar"
}
mapeo_clasificacion2 = {
"20202.1.1.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.1.1.1.1.2PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.1.1.3.1.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.1.1.3.3.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.1.1.3.3.4PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.1.1.9.1.3PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.1.1.9.3.98PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.1.3.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.1.3.1.1.3PAGO DE CUOTAS DEL REPRO - AFP":"No",
"20202.1.3.1.1.3PAGO DE DEUDA AFP":"No",
"20202.1.3.1.1.3DEUDA DE ACOGIMIENTO AL REPRO AFP":"No",
"20202.1.3.1.1.5PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.1.3.1.1.5PAGO DE CUOTAS DEL REPRO - AFP":"No",
"20202.1.3.1.1.6PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.3.1.5.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20202.3.2.8.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20202.3.2.8.1.2PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20202.3.2.8.1.4PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20202.3.2.8.1.5PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20202.5.2.1.1.99TRANSFERENCIA A LAS AFP POR PAGOS INDEBIDOS DE LOS GOBIERNOS REGIONALES AL SISTEMA NACIONAL DE PENSIONES":"No",
"20202.5.4.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.5.4.1.2.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.5.4.1.3.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.5.4.1.3.1PAGO DE FRACCIONAMIENTO POR DEUDAS A LA SUNAT, AFP U OTROS":"No",
"20202.5.4.1.3.1PAGO DE FRACCIONAMIENTO DE DEUDAS A LA SUNAT, AFP Y OTROS":"No",
"20202.5.4.3.2.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.5.5.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.5.5.1.1.1PAGO DE FRACCIONAMIENTO POR DEUDAS A LA SUNAT, AFP U OTROS":"No",
"20202.5.5.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20202.6.3.2.1.2PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20202.6.3.2.3.1PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20202.6.3.2.3.3PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20202.6.3.2.9.4PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20202.6.6.1.3.99PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20212.1.1.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20212.1.1.9.3.99PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20212.1.3.1.1.2PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20212.1.3.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20212.1.3.1.1.3PAGO DE CUOTAS DEL REPRO - AFP":"No",
"20212.1.3.1.1.3PAGO DE CUOTAS POR EL ACOGIMIENTO A REPRO AFP II":"No",
"20212.1.3.1.1.3PAGO DE DEUDA AFP":"No",
"20212.1.3.1.1.3DEUDA DE ACOGIMIENTO AL REPRO AFP":"No",
"20212.1.3.1.1.3PAGO DE CUOTAS DEL SINCERAMIENTO DE DEUDAS POR APORTACIONES A LA AFP":"No",
"20212.1.3.1.1.5PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20212.1.3.1.1.5PAGO DE CUOTAS DEL REPRO - AFP":"No",
"20212.1.3.1.1.6PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20212.3.2.7.11.99PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20212.3.2.8.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20212.3.2.8.1.2PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20212.3.2.8.1.4PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20212.3.2.8.1.5PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20212.5.2.1.1.99TRANSFERENCIA A LAS AFP POR PAGOS INDEBIDOS DE LOS GOBIERNOS REGIONALES AL SISTEMA NACIONAL DE PENSIONES":"No",
"20212.5.4.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20212.5.4.1.3.1PAGO DE FRACCIONAMIENTO POR DEUDAS A LA SUNAT, AFP U OTROS":"No",
"20212.5.5.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20212.5.5.1.1.1PAGO DE FRACCIONAMIENTO POR DEUDAS A LA SUNAT, AFP U OTROS":"No",
"20212.5.5.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20212.5.5.1.2.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20222.1.1.1.1.2PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20222.1.1.9.3.99PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20222.1.3.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20222.1.3.1.1.3PAGO DE CUOTAS DEL REPRO - AFP":"No",
"20222.1.3.1.1.3PAGO DE CUOTAS POR EL ACOGIMIENTO A REPRO AFP II":"No",
"20222.1.3.1.1.3PAGO DE DEUDA AFP":"No",
"20222.1.3.1.1.3PAGO DE CUOTAS DEL SINCERAMIENTO DE DEUDAS POR APORTACIONES A LA AFP":"No",
"20222.1.3.1.1.5PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20222.1.3.1.1.5PAGO DE CUOTAS DEL REPRO - AFP":"No",
"20222.3.1.5.1.2PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20222.3.1.99.1.99PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20222.3.2.1.2.1PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20222.3.2.1.2.2PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20222.3.2.7.11.99PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20222.3.2.8.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20222.5.2.1.1.99PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20222.5.2.1.1.99TRANSFERENCIA A LAS AFP POR PAGOS INDEBIDOS DE LOS GOBIERNOS REGIONALES AL SISTEMA NACIONAL DE PENSIONES":"No",
"20222.5.4.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20222.5.4.2.2.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20222.5.5.1.3.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20222.6.3.2.3.1PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20232.1.1.1.1.2PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20232.1.1.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20232.1.1.9.3.99PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20232.1.3.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20232.1.3.1.1.3PAGO DE CUOTAS DEL REPRO - AFP":"No",
"20232.1.3.1.1.3ACOGIMIENTO AL REPRO AFP Y DEUDA CON ESSALUD DE LA MUNCIPALIDAD DISTRITAL DE OLMOS":"No",
"20232.1.3.1.1.3PAGO DE CUOTAS DEL SINCERAMIENTO DE DEUDAS POR APORTACIONES A LA AFP Y ONP":"No",
"20232.1.3.1.1.3PAGO DE CUOTAS DEL SINCERAMIENTO DE DEUDAS POR APORTACIONES A LA AFP":"No",
"20232.1.3.1.1.5PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20232.1.3.1.1.5ACOGIMIENTO AL REPRO AFP Y DEUDA CON ESSALUD DE LA MUNCIPALIDAD DISTRITAL DE OLMOS":"No",
"20232.1.3.1.1.5PAGO DE CUOTAS DEL REPRO - AFP":"No",
"20232.1.3.1.1.6PAGO DE CUOTAS DEL REPRO - AFP":"No",
"20232.3.2.7.11.99PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20232.3.2.9.1.1PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20232.5.4.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20232.5.4.3.3.1PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20232.5.5.1.3.1PAGO DE CUOTAS DEL REPRO - AFP":"No",
"20232.6.3.2.3.3PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20242.1.1.1.1.2PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20242.1.1.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20242.1.1.9.3.99PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20242.1.3.1.1.13PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20242.1.3.1.1.13ACOGIMIENTO AL REPRO AFP Y DEUDA CON ESSALUD DE LA MUNCIPALIDAD DISTRITAL DE OLMOS":"No",
"20242.1.3.1.1.13PAGO DE CUOTAS DEL REPRO - AFP":"No",
"20242.1.3.1.1.14PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20242.1.3.1.1.3PAGO DE CUOTAS DEL REPRO-AFP":"No",
"20242.1.3.1.1.3PAGO DE CUOTAS DEL REPRO - AFP":"No",
"20242.1.3.1.1.3ACOGIMIENTO AL REPRO AFP Y DEUDA CON ESSALUD DE LA MUNCIPALIDAD DISTRITAL DE OLMOS":"No",
"20242.1.3.1.1.3PAGO DE CUOTAS DEL SINCERAMIENTO DE DEUDAS POR APORTACIONES A LA AFP Y ONP":"No",
"20242.1.3.1.1.3PAGO DE CUOTAS DEL SINCERAMIENTO DE DEUDAS POR APORTACIONES A LA AFP":"No",
"20242.3.2.7.11.99PAGO DE CUOTAS DEL REPRO-AFP":"Sí",
"20242.5.4.1.1.1SINCERAMIENTO DE DEUDAS ESSALUD Y ONP Y REPRO-AFP":"No",
"20242.5.4.1.1.1PAGO DE CUOTAS DEL REPRO-AFP":"No"
}
# Definir la función de clasificación
def clasificar_ruta(clasificacion):
    return mapeo_clasificacion.get(clasificacion, 'Sin definir')

def clasificar_ruta2(clasificacion):
    return mapeo_clasificacion2.get(clasificacion, 'Sin definir')
# Aplicar la función de clasificación para crear el nuevo campo
df_melted['_CALIDAD_RUTA_CLASIFICACION'] = df_melted['_COMP_CADENA'].map(clasificar_ruta)
df_melted['_CALIDAD_BSINV_CLASIFICACION'] = df_melted['_COMP_CADENA'].map(clasificar_ruta2)

########################################################################################################################
df_melted['_CALIDAD_BSINV_CLASIFICACION'].unique()
########################################################################################################################
# Diccionario de mapeo de clasificación _META_NOMBRE_COH_REPROAFP
mapeo_meta_nombre = {
    'ACOGIMIENTO AL REPRO AFP Y DEUDA CON ESSALUD DE LA MUNCIPALIDAD DISTRITAL DE OLMOS': 'SI',
    'DEUDA DE ACOGIMIENTO AL REPRO AFP': 'SI',
    'PAGO DE CUOTAS DEL REPRO - AFP': 'SI',
    'PAGO DE CUOTAS DEL REPRO-AFP': 'SI',
    'PAGO DE CUOTAS DEL SINCERAMIENTO DE DEUDAS POR APORTACIONES A LA AFP': 'SI',
    'PAGO DE CUOTAS DEL SINCERAMIENTO DE DEUDAS POR APORTACIONES A LA AFP Y ONP': 'SI',
    'PAGO DE CUOTAS POR EL ACOGIMIENTO A REPRO AFP II': 'SI',
    'PAGO DE DEUDA AFP': 'SI',
    'PAGO DE FRACCIONAMIENTO DE DEUDAS A LA SUNAT, AFP Y OTROS': 'SI',
    'PAGO DE FRACCIONAMIENTO POR DEUDAS A LA SUNAT, AFP U OTROS': 'SI',
    'SINCERAMIENTO DE DEUDAS ESSALUD Y ONP Y REPRO-AFP': 'SI',
    'TRANSFERENCIA A LAS AFP POR PAGOS INDEBIDOS DE LOS GOBIERNOS REGIONALES AL SISTEMA NACIONAL DE PENSIONES': 'NO',

}


# Definir la función de clasificación
def clasificar_meta_nombre(clasificacion):
    return mapeo_meta_nombre.get(clasificacion, 'SIN_DEFINIR')

# Aplicar la función de clasificación para crear el nuevo campo
df_melted['_META_NOMBRE_COH_REPROAFP'] = df_melted['META_NOMBRE'].map(clasificar_meta_nombre)
########################################################################################################################
# eliminacion de todos los devengados negativos

df_melted['_DEVENGADO_MES_SIGNO'] = df_melted['_DEVENGADO_MES'].apply(lambda x: np.where(x < 0, 'Negativo', 'Positivo'))



########################################################################################################################
df_melted.to_excel("ouput/Resultados_preliminares_APF_"+date_time_str+".xlsx")




########################################################################################################################
