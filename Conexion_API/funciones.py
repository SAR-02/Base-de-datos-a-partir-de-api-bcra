import pandas as pd
import pyodbc
def tipo_de_dato(dtype): # para que 
    if "int" in str(dtype):
        return "INT"
    elif "float" in str(dtype):
        return "FLOAT"
    elif "datetime" in str(dtype):
        return "DATETIME"
    else:
        return "VARCHAR(MAX)"

def crear_tabla_sql(server,base_de_datos,df,nombre_tabla,):
    df.columns = [c.strip()[:120] for c in df.columns]#limito len de nombre de columnas, si no se hace falla
    server=f'{server}'
    db=f'{base_de_datos}'
    
    conexion = pyodbc.connect( #conexion a db,revisar 
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={db};"
    "Trusted_Connection=yes;")

    cursor=conexion.cursor()

    partes_columnas = [f"[{col}] {tipo_de_dato(df[col].dtype)}" for col in df.columns]
    definicion_columnas = ", ".join(partes_columnas)
    query_create = f"""
    IF OBJECT_ID('{nombre_tabla}', 'U') IS NOT NULL DROP TABLE {nombre_tabla};
    CREATE TABLE {nombre_tabla} ({definicion_columnas})
    """
    cursor.execute(query_create)

    df_sql = df.astype(object).where(pd.notnull(df), None)

    valores = [tuple(x) for x in df_sql.values]

    columnas_str = ", ".join([f"[{c}]" for c in df.columns])
    placeholders = ", ".join(["?"] * len(df.columns))
    query_insert = f"INSERT INTO {nombre_tabla} ({columnas_str}) VALUES ({placeholders})"

    cursor = conexion.cursor()
    cursor.fast_executemany = True
    cursor.executemany(query_insert, valores)

    conexion.commit() 
