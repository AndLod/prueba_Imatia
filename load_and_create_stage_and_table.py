from argparse import Namespace
import argparse
import os
from pathlib import Path
import sys

from snowflake.connector.cursor import SnowflakeCursor
import pandas as pd

from connections import create_con
from utils import get_file_path_name

# Constante del esquema
SCHEMA = "PUBLIC"
NUM_ROWS_TO_READ = 100

# Diccionario con la equivalencia de tipo de pandas y snowflake
PANDAS_TO_SNOWFLAKE = {
    "int64": "NUMBER",
    "Int64": "NUMBER",
    "float64": "FLOAT",
    "bool": "BOOLEAN",
    "boolean": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "object": "STRING",
    "string": "STRING",
    "category": "STRING"
}


### Función para los parámetros de entrada de creación y carga del stage
def get_args() -> Namespace:
    """
    Función para los parámetros de entrada de creación y carga del stage.

    :return: Argumentros de entrada.
    :rtype: Namespace
    """ 
    description = "Creación y carga del stage"
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('-b', '--database', dest="db", required=True, help='Nombre de la base de datos utilizar')
    parser.add_argument('-c', '--csv_parth', dest="csv_path", required=True, help='Ruta a la carpeta del csv o fichero csv')

    return parser.parse_args()


### Función para subir un fichero csv a un stage
def put_file_to_stage(cursor: SnowflakeCursor, csv_path: str, stage_name: str):
    """
    Función para subir un fichero csv a un stage.

    :param cursor: Cursor de snowfake.
    :type cursor: SnowflakeCursor
    :param csv_path: Nombre de la base de datos.
    :type csv_path: str
    :param stage_name: Ruta a la carpeta del csv.
    :type stage_name: str
    """
    full_path = csv_path.replace("\\", '/')
    put_cmd = f"PUT 'file://{full_path}' @{stage_name} AUTO_COMPRESS=TRUE"
    cursor.execute(put_cmd)


### Se calculan los tipos de la tabla a partir de X lineas del csv 
def get_csv_columns_types(path: str, nrows: int) -> dict[str, str]:
    """
    Se devuelve un diccionario con los tipos del csv.

    :param path: Ruta al csv.
    :type path: str
    :param nrows: Número de filas a procesar del csv.
    :type nrows: int
    
    :return: El diccionario con los tipos de las columnas.
    :rtype: dict[str, str]
    """

    csv_path = ''
    dir_path = Path(path)
    # En caso de ser un fichero
    if dir_path.is_file():
        csv_path = path
    
    # Es una ruta a un directorio con csv
    elif dir_path.is_dir():
        for file in os.scandir(path):
            if file.is_file() and file.name.lower().endswith('.csv'):
                csv_path = file.path
                break
    else:
        print(f"ERROR: La ruta {path} no es válida.")
        sys.exit(1)

    # Se cargan solo las primeras X lineas del csv y calcula cual es el separador
    df = pd.read_csv(csv_path, nrows=nrows, encoding='utf-8', sep=None, engine='python')

    # Tipos mapeados a snowflake desde pandas
    column_types_snowflake = {
        str(col): PANDAS_TO_SNOWFLAKE[str(dtype)]
        for col, dtype in df.dtypes.items()
    }

    return column_types_snowflake


### Se carga el contenido al stage del snowflake
def create_and_load_stage(cursor: SnowflakeCursor, database_name: str, csv_path: str) -> str:
    """
    Se crea y carga el stage, y se devuelve el nombre del stage creado.

    :param cursor: Cursor de snowfake.
    :type cursor: SnowflakeCursor
    :param database_name: Nombre de la base de datos.
    :type database_name: str
    :param csv_path: Ruta a la carpeta del csv.
    :type csv_path: str
    
    :return: Nombre del stage creado.
    :rtype: str
    """
    stage_name = f"{get_file_path_name(csv_path)}_stage"

    # Se usan
    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"USE SCHEMA {SCHEMA}")

    # Se borra el stage si existía
    cursor.execute(f"DROP STAGE IF EXISTS {stage_name}")
    cursor.execute(
        f"CREATE STAGE {stage_name} "
        f" DIRECTORY = (ENABLE = TRUE);"
    )
    print(f"\n- Stage {stage_name} creado en {database_name}.{SCHEMA}.")

    print(f"\n- Cargando el stage {stage_name} ...")
    # Se comprueba si es un fichero
    dir_path = Path(csv_path)
    if dir_path.is_file():
        print(f"\t-> Subiendo el {dir_path.name} ...")
        put_file_to_stage(cursor, csv_path, stage_name)
        print(f"- {stage_name} cargado.")
    
    elif dir_path.is_dir():
        # Subir todos los CSV del directorio
        for file in os.scandir(csv_path):
            if file.is_file() and file.name.lower().endswith('.csv'):
                print(f"\t-> Subiendo {file} ...")
                put_file_to_stage(cursor, file.path, stage_name)

        print(f"- {stage_name} cargado.")
        print(f"- Todos los CSV de {csv_path} han sido subidos al stage.")

    else:
        print(f"ERROR: La ruta {csv_path} no es válida.")
        sys.exit(1)

    return stage_name


### Se crea la tabla y se carga con el contenido del stage
def create_and_load_table(cursor: SnowflakeCursor, table_name: str, stage_name: str, columns_types: dict[str, str]):
    print(f"\n- Creando la tabla {table_name} ...")
    columns_sql = ", ".join([f'"{col}" {typ}' for col, typ in columns_types.items()])
    create_sql = f"CREATE OR REPLACE TABLE {table_name} ({columns_sql});"
    cursor.execute(create_sql)

    print("- Creando el format ...")
    table_format_name = f"{table_name}_format"
    table_format = f"""
        CREATE OR REPLACE FILE FORMAT {table_format_name}
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
        NULL_IF = ('NULL', '')
        TRIM_SPACE = TRUE;
    """
    cursor.execute(table_format)

    print(f"- Cargando el contenido de {stage_name} en la tabla {table_name}")
    load_table = f"""
        COPY INTO {table_name}
        FROM @{stage_name}
        FILE_FORMAT = {table_format_name}
        ON_ERROR = 'CONTINUE';
    """
    cursor.execute(load_table)


if __name__ == "__main__":
    # Se obtienen los argumentos de entrada
    args = get_args()

    if args.db.strip() == "":
        print(f"ERROR: Se debe escribir un nombre de base de datos válido: {args.db}.")
        sys.exit(1)

    if args.csv_path.strip() == "":
        print(f"ERROR: Se debe escribir la ruta a la carpeta del csv: {args.csv_path}.")
        sys.exit(1)

    # Se obtiene la ruta al csv
    csv_path = args.csv_path

    print(f"\n- Estableciendo conexión con snowflake ...")
    conn = create_con()
    cursor = conn.cursor()
    
    print(f"\nInicio de la creación y carga del stage:")
    stage_name = create_and_load_stage(cursor, args.db, csv_path)
    print(f"\nFin creación y carga del stage.")

    # Se obtiene el nombre de la tabla a crear => nombre del fichero
    table_name = get_file_path_name(csv_path)

    # Se obtienen los tipos de las columnas de la tabla a crear
    columns_types = get_csv_columns_types(csv_path, NUM_ROWS_TO_READ)

    print(f"\nInicio de la creación y carga de la tabla {table_name}:")
    create_and_load_table(cursor, table_name, stage_name, columns_types)
    print(f"\nFin creación y carga de la tabla {table_name}.")
    
    cursor.close()
    conn.close()






