-- Se necesita para poder emplear el TRANSLATE
-- Permite que la región en la que se encuentra la cuenta pueda emplear la 
--  función de traducción
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- Función de create de la tabla
CREATE OR REPLACE TABLE FINAL_TABLE AS
WITH BASE AS (
    SELECT --TOP 50
        R.LOCAL_DATE,
        C.COUNTRY_ISO,
        C.COUNTRY,
        SPLIT_PART(R.DEVICE, '-', 1) AS DEVICE,
        SPLIT_PART(R.DEVICE, '-', 2) AS SO,
        R.SECTION AS UNIVERSO,
        LOWER(TRIM(SPLIT_PART(R.LANGUAGE, '_', 1))) AS LANGUAGE,
        CASE WHEN TRY_TO_NUMBER(R.SEARCH_QUERY) IS NOT NULL 
             THEN 'REFERENCE' ELSE 'TEXT' END AS QUERY_TYPE,
        R.SEARCH_QUERY,
        CASE 
            WHEN TRY_TO_NUMBER(R.SEARCH_QUERY) IS NULL 
                 AND LOWER(TRIM(SPLIT_PART(R.LANGUAGE, '_', 1))) IN 
                     ('zh','nl','en','fr','de','hi','it','ja','ko','pl','pt','ru','es','sv')
            THEN LOWER(TRIM(SPLIT_PART(R.LANGUAGE, '_', 1)))
            ELSE NULL
        END AS LANGUAGE_OK,
        R.SEARCHS,
        R.SESSIONS
    FROM RECORDS R
    INNER JOIN COUNTRIES C
        ON C.COUNTRY = R.COUNTRY
)
SELECT 
    LOCAL_DATE,
    COUNTRY_ISO,
    MAX(COUNTRY) AS COUNTRY_NAME,
    DEVICE,
    SO,
    UNIVERSO,
    LANGUAGE,
    QUERY_TYPE,
    SEARCH_QUERY,
    MAX(
        CASE 
            WHEN QUERY_TYPE = 'TEXT' AND LANGUAGE_OK IS NOT NULL THEN SNOWFLAKE.CORTEX.TRANSLATE(SEARCH_QUERY, LANGUAGE_OK, 'es')
            ELSE SEARCH_QUERY
        END
    ) AS SEARCH_QUERY_TRANSLATED,
    MAX(SEARCHS) AS SEARCHS,
    MAX(SESSIONS) AS SESSIONS
FROM BASE
GROUP BY 
    LOCAL_DATE, COUNTRY_ISO, DEVICE, SO, UNIVERSO, LANGUAGE, QUERY_TYPE, SEARCH_QUERY;

-- select de la tabla final
SELECT * FROM FINAL_TABLE