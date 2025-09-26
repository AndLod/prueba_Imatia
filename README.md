# Prueba Imatia

## Preparación del entorno

### 1. Crear del entorno virtual
> `python -m venv .venv`

### 2. Activar el entorno
> `.\.venv\Scripts\activate`

### 3. Actualizar pip
> `python.exe -m pip install --upgrade pip`

### 4. Ejecutar el fichero `requirements.txt`
> `pip install -r requirements.txt`


---

## Explicación scripts python

- `connections.py`: Fichero donde se encuentran la función para realizar la conexión al _snowflake_. Antes de la ejecución de los _scripts_ se deberán actualizar los valores de las constantes `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ACCOUNT`, con los del propio usuario.

- `split_csv.py`: Script encargado en preprocesar y particionar un fichero csv en varios de menor tamaño. El usuario indicará la ruta al fichero que quiere particionar a continuación del _flag_ `-p`, además puede indicar el tamaño máximo en megabytes de cada uno de estos ficheros csv que se generarán mediante el _flag_ `-s`. Tarda bastante en generar las particiones, entre unos 5-10 minutos.

- `create_database.py`: Script encargado de crear en `snowflake` la base de datos que se va a utilizar. En caso de existir una con el mismo nombre, la elimina y vuelve a crear. El usuario indica mediante el _flag_ `-b` el nombre de la base de datos que desea crear.

- `load_and_create_stage_and_table.py`: Script encargado de crear y cargar en _snowflake_ el _stage_ con el o los csv que lo formen y la tabla relacionada con este _stage_. En caso de que ya existan, se borrarán y volverán a crear y cargar. Tanto el stage como la tabla se creará con el mismo nombre del csv o carpeta a los csv que se indique. El usuario indica mediante el _flag_ `-c` la ruta al fichero csv o directorio de los ficheros csv que quiere cargar, a mayores, con el _flag_ `-b` tiene que indicar el nombre de la base de datos donde se debe crear la tabla. Dicho script es el que más tarda en ejecutarse, debido a la subida de los ficheros csv al _stage_, tardando alrededor de 10 minutos.

- `utils.py`: Fichero python con las funciones auxiliares necesarias para la ejecución.

---

## Orden y forma de ejecución de los scripts

### 1. Particionar el csv `records.csv`
> python .\split_csv.py -p __[RUTA AL CSV `records.csv`]__ -s __[TAMAÑO MÁXIMO DE CADA PARTICIÓN]__

### 2. Crear la base de datos
> python .\create_database.py -b __[NOMBRE BASE DE DATOS]__

### 3. Creación y carga del _stage_ y tabla de COUNTRIES
> python .\load_stage_and_create_table.py -c __[RUTA AL CSV `countries.csv`]__ -b __[NOMBRE BASE DE DATOS]__

### 4. Creación y carga del _stage_ y tabla de RECORDS
> python .\load_stage_and_create_table.py -c __[RUTA AL DIRECTORIO CON PARTICIONES DE `records.csv`]__ -b __[NOMBRE BASE DE DATOS]__

--- 

## Explicación consulta SQL
La consulta que crea la tabla pedida en el enunciado se encuentra en el fichero `create_table.sql`. Comentar que para la traducción de la columna `search_query` se emplea la función `SNOWFLAKE.CORTEX.TRANSLATE` que tiene ciertas limitaciones: 
- En mi caso, se tuvo que activar/permitir que mi cuenta alojada en la región pudiera emplear dicha función de traducción.
- No se encuentran todos los idiomas que se incluyen en las filas.
- Es demasiado lenta para la cantidad de datos que tiene la tabla.

Para emplear la función de traducción se tiene que ejecutar antes: 
> `ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';`

En la [documentación de dicha función](https://docs.snowflake.com/en/sql-reference/functions/translate-snowflake-cortex) se comentan los idiomas disponibles, siendo estos los que se indicaron manualmente en la consulta, ya que no encontré forma de obtenerlos mediante una consulta. 

Por lo tanto recomiendo descomentar la clave `TOP 50`, que se encuentra en la sentencia `SELECT` del `WITH` para probar que funciona correctamente.


