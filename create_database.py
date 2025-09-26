from argparse import Namespace
import argparse
import sys

from snowflake.connector.cursor import SnowflakeCursor
from connections import create_con


### Función obtener como parámetro de entrada el nombre de la base de datos
def get_args() -> Namespace:
    """
    Función obtener como parámetro de entrada el nombre de la base de datos.

    :return: Argumentros de entrada.
    :rtype: Namespace
    """ 
    description = "Script para la crear la base de datos"
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('-b', '--database', dest="bd", required=True, help='Nombre de la base de datos a crear')

    return parser.parse_args()


### Se crea la DATABASE 
def create_database(cursor: SnowflakeCursor, db_name: str):
    """
    Se crea la base de datos.

    :param cursor: Cursor de snowfake.
    :type cursor: SnowflakeCursor
    :param db_name: Nombre de la base de datos.
    :type db_name: str
    """ 
    # Se comprueba que la DATABASE existe y se borra
    print(f"\n- Comprobando si existe la database {db_name} ...")
    cursor.execute(f"SHOW DATABASES LIKE '{db_name}'")
    if cursor.fetchone():
        cursor.execute(f"DROP DATABASE {db_name}")
        print(f"\t-> Database {db_name} borrada.")

    # Se crea la DATABASE
    print(f"- Creando la database {db_name} ...")
    cursor.execute(f"CREATE DATABASE {db_name}")
    print(f"\t-> Database {db_name} creada.")


if __name__ == "__main__":
    # Se obtienen los argumentos de entrada
    args = get_args()

    if args.bd.strip() == "":
        print(f"ERROR: Se debe escribir un nombre de base de datos válido: {args.bd}.")
        sys.exit(1)

    print(f"\n- Estableciendo conexión con snowflake ...")
    conn = create_con()
    cursor = conn.cursor()

    create_database(cursor, args.bd)

    cursor.close()
    conn.close()
