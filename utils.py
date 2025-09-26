
from pathlib import Path

### Devuelve el nombre del archivo sin la extension
def get_file_path_name(file_path_str: str) -> str:
    """
    De un path devuelve el nombre del archivo sin la extension.

    :param file_path_str: La ruta al fichero.
    :type file_path_str: str

    :return: Nombre del archivo sin suextensión.
    :rtype: str
    """ 
    file_path = Path(file_path_str)

    # nombre de la carpeta sin extensión
    return file_path.stem