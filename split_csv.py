from argparse import Namespace
import argparse
from io import TextIOWrapper
import os
from pathlib import Path
import shutil
import sys
import csv

from utils import get_file_path_name

SCRIPT_PATH = os.path.realpath(__file__)
PARENT_DIR = os.path.dirname(SCRIPT_PATH)

# Constantes
FILE_ENCODE = "utf-8"

### Función para los parámetros de entrada de creación y carga del stage
def get_args() -> Namespace:
    """
    Función para configurar y obtener los parámetros de entrada del script.

    :return: Argumentros de entrada.
    :rtype: Namespace
    """ 
    description = "Script para la partición de un csv en partes según su tamaño"
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('-p', '--path_csv', dest="path", required=True, help='Ruta del csv')
    parser.add_argument('-s', '--size_part', dest="size", required=False, help='Tamaño máximo de la particion en MB')

    return parser.parse_args()


### Crear el directorio de salida
def create_dir(dir_path: str):
    """
    Intenta crear un nuevo directorio, en caso de que ya 
    exista lo borra y vuelve a crear.

    :param file_path_str: Ruta al csv a procesar.
    :type file_path_str: str
    """
    print(f"\n- Comprobando si existe el directorio {dir_path} ...")
    if os.path.exists(dir_path):
        print(f"\t-> Directorio {dir_path} borrado.")
        shutil.rmtree(dir_path)
        os.makedirs(dir_path)
    else:
        os.makedirs(dir_path)

    print(f"- Directorio {dir_path} creado.")


### Se obtiene el csv parte de destino
def get_csv_part_out(origin_file_name: str, output_dir: str, part_num: int, 
                     extension: str, file_encode: str) -> tuple[str, TextIOWrapper]:
    """
    Se obtiene y abre el fichero "particionado" de destino.
    Se devuelve el nombre del fichero y el fichero abierto.

    :param origin_file_name: Nombre del fichero de origen.
    :type origin_file_name: str
    :param output_dir: Ruta del directorio de destino.
    :type output_dir: str
    :param part_num: Número de parte del fichero.
    :type part_num: int
    :param extension: Extensión del archivo.
    :type extension: str
    :param file_encode: Codificación del fichero de destino.
    :type file_encode: str
    
    :return: Nombre del fichero y el fichero abierto.
    :rtype: tuple[str, TextIOWrapper]
    """ 
    file_name_part = f"{origin_file_name}_part{part_num}{extension}"
    out_path = os.path.join(output_dir, file_name_part)
    return file_name_part, open(out_path, "w", encoding=file_encode, newline='')


### Se obtiene el separador del fichero csv
def get_csv_delimiter(file: TextIOWrapper) -> str:
    """
    Se obtiene el separador del fichero.

    :param file: Fichero a obtener el separador.
    :type file: TextIOWrapper
    
    :return: Separador del fichero.
    :rtype: str
    """ 
    # Se lee parte del csv para detectar el delimitador del mismo
    sample = file.read(2048)
    # Se vuelve al inicio del fichero
    file.seek(0)
    return csv.Sniffer().sniff(sample).delimiter


### Se preprocesa el csv
def clean_csv(file_path_str: str) -> str:
    """
    Se eliminan los caracteres especiales y raros del csv.

    :param file_path_str: Ruta al csv a procesar.
    :type file_path_str: str

    :return: Path al csv preprocesado.
    :rtype: str
    """ 
    
    # Fichero de entrada
    file_path = Path(file_path_str)

    # Ruta al fichero procesado
    clean_path = os.path.join(file_path.parent, "records_clean.csv")

    print(f"\n- Limpiando el csv ...")
    with open(file_path_str, "rb") as f_in, open(clean_path, "wb") as f_out:
        for chunk in iter(lambda: f_in.read(1024*1024), b""):
            f_out.write(chunk.replace(b"\x00", b""))

    return clean_path


### Dividir el csv en partes de 250 MB como máximo
def split_csv(file_path_str: str, output_dir: str, max_chunk_size: int=250, file_encode: str = FILE_ENCODE):
    """
    Divide un CSV en trozos de ~max_chunk_size bytes.
    Mantiene la cabecera solo en la primera parte.

    :param file_path_str: Ruta al csv a procesar.
    :type file_path_str: str
    :param output_dir: Directorio de destino de los csv.
    :type output_dir: str
    :param max_chunk_size: Tamaño máximo de cada parte del csv.
    :type max_chunk_size: int
    :param file_encode: Encoding del fichero csv.
    :type file_encode: str
    """ 
    max_chunk_size = max_chunk_size*1024*1024

    file_path = Path(file_path_str)
    file_name = file_path.stem.split('_')[0]
    extension = file_path.suffix

    print(f"\n- Iniciando la partición de los csv ...")
    
    part_num = 1
    current_size = 0

    # Se obtiene el nombre del nuevo fichero csv y el propio fichero listo para escribir en el
    file_name_part, out_file = get_csv_part_out(file_name, output_dir, part_num, extension, file_encode)

    print(f"\t- Generando el csv {file_name_part} ...")
    
    with file_path.open("r", encoding=file_encode, newline="", errors="replace") as f:
        # Se obtiene el separador del csv
        delimiter = get_csv_delimiter(f)

        # Se comienza a leer el fichero csv
        reader = csv.reader(f, delimiter=delimiter, quotechar='"')

        # Se lee la cabecera del csv
        header_list = next(reader)

        # Se crea el writer del csv nuevo
        writer = csv.writer(
            out_file, delimiter=delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator="\n"
        )

        # Se escribe la cabecera en el nuevo csv
        writer.writerow(header_list)

        # Se actualiza el tamaño actual
        current_size += len(",".join(header_list).encode(file_encode))

        # Se recorre el fichero
        for row in reader:
            row = [col.replace("\n", " ") for col in row]

            # Se calcula el tamaño de línea actual
            row_size = len(",".join(row).encode(file_encode))

                # Si el tamaño del fichero superase el tamaño máximo permitido
            if current_size + row_size > max_chunk_size:
                # Se cierra el fichero actual de escritura
                out_file.close()

                part_num += 1

                # Se obtiene el nombre del nuevo fichero csv ...
                # ... y el propio fichero listo para escribir en el
                file_name_part, out_file = get_csv_part_out(
                    file_name, output_dir, part_num, extension, file_encode
                )
                # Se crea el writer del csv nuevo
                writer = csv.writer(
                    out_file, delimiter=delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator="\n"
                )

                # Se escribe la cabecera en el nuevo csv
                writer.writerow(header_list)

                current_size = 0
                print(f"\t- Generando el csv {file_name_part} ...")

            writer.writerow(row)
            current_size += row_size

    out_file.close()
    print(f"CSV dividido en {part_num} partes en la carpeta '{output_dir}'.")


if __name__ == "__main__":
    args = get_args()

    # Comprobaciones de los parámetros de entrada
    if args.path.strip() == "":
        print(f"ERROR: Se debe escribir la ruta a la carpeta del csv: {args.path}.")
        sys.exit(1)
    
    file = args.path
    if not os.path.exists(file):
        print(f"El archivo '{file}' no existe.")
        sys.exit(1)

    size_part = int(args.size)
    if size_part <= 0:
        print(f"ERROR: El tamaño máximo de la partición debe ser mayor a 0: {args.size}.")
        sys.exit(1)

    print(f"Se inicia el procesamiento para particionar el csv ...")

    output_path_dir = os.path.join(PARENT_DIR,  get_file_path_name(file))
    create_dir(output_path_dir)

    # Limpieza de caracteres raros del csv
    file_clean = clean_csv(file)

    # Particion del csv
    split_csv(file_clean, output_path_dir, max_chunk_size=size_part)
    
    print("\nFin particion del csv.")