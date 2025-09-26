import snowflake.connector
from snowflake.connector import SnowflakeConnection

# Constantes para la conexión
SNOWFLAKE_USER = "USER" # Usuario
SNOWFLAKE_PASSWORD = "PASSWORD" # Contraseña
SNOWFLAKE_ACCOUNT = "ACCOUNT" # Account

### Se crea la conexión a snowflake
def create_con() -> SnowflakeConnection:
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT
    )