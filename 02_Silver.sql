-- Databricks notebook source
-- DBTITLE 1,1. Creación de tabla "Persona" en la capa Silver
DROP DATABASE IF EXISTS SILVER_RETAIL CASCADE;

CREATE SCHEMA IF NOT EXISTS SILVER_RETAIL;

DROP TABLE IF EXISTS SILVER_RETAIL.CLIENTE;

CREATE TABLE SILVER_RETAIL.CLIENTE(
ID STRING,
NOMBRE STRING,
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING,
FECHA_CARGA DATE
)
USING DELTA
LOCATION 'dbfs:/mnt/lakehousedata/silver/persona/'
;

-- COMMAND ----------

-- DBTITLE 1,2. Carga curada a la tabla "Persona" en la capa Silver
INSERT OVERWRITE TABLE SILVER_RETAIL.CLIENTE
SELECT 
  TRIM(ID)
, TRIM(NOMBRE)
, NVL(EDAD,0)
, NVL(SALARIO,0)
, TRIM(ID_EMPRESA)
, CURRENT_DATE()
FROM BRONZE_RETAIL.CLIENTE;

-- COMMAND ----------

-- DBTITLE 1,3. Validación de datos cargados a la tabla "Persona" en la capa Silver
SELECT * FROM SILVER_RETAIL.CLIENTE;

-- COMMAND ----------

-- DBTITLE 1,4. Creación de tabla "Empresa" asociada al directorio del archivo
DROP TABLE IF EXISTS SILVER_RETAIL.EMPRESA;

CREATE TABLE SILVER_RETAIL.EMPRESA(
    ID STRING,
    NOMBRE STRING,
    FECHA_CARGA DATE
)
USING DELTA
LOCATION 'dbfs:/mnt/lakehousedata/silver/empresa/'
;

-- COMMAND ----------

-- DBTITLE 1,5. Carga curada a la tabla "Empresa" en la capa Silver
-- MAGIC %py
-- MAGIC spark.sql("INSERT OVERWRITE TABLE SILVER_RETAIL.EMPRESA \
-- MAGIC             SELECT \
-- MAGIC               TRIM(ID) \
-- MAGIC             , TRIM(NOMBRE) \
-- MAGIC             , CURRENT_DATE() \
-- MAGIC             FROM BRONZE_RETAIL.EMPRESA")

-- COMMAND ----------

-- DBTITLE 1,6. Validación de datos cargados a la tabla "Empresa" en la capa Silver
-- MAGIC %py
-- MAGIC dfEmpresa = spark.sql("SELECT * FROM SILVER_RETAIL.EMPRESA")
-- MAGIC
-- MAGIC dfEmpresa.show(10)
-- MAGIC
-- MAGIC dfEmpresa.describe().show()

-- COMMAND ----------

-- DBTITLE 1,7. Creación de tabla "Transaccion" en la capa Silver - PARQUET
DROP TABLE IF EXISTS SILVER_RETAIL.VENTA_PARQUET;

CREATE TABLE SILVER_RETAIL.VENTA_PARQUET(
ID_PERSONA STRING,
ID_EMPRESA STRING,
MONTO DOUBLE
)
STORED AS PARQUET
PARTITIONED BY (FECHA STRING)
LOCATION 'dbfs:/mnt/lakehousedata/silver/venta_parquet/'
;

-- COMMAND ----------

-- DBTITLE 1,7. Creación de tabla "Transaccion" en la capa Silver - DELTA
DROP TABLE IF EXISTS SILVER_RETAIL.VENTA;

CREATE TABLE SILVER_RETAIL.VENTA(
ID_PERSONA STRING,
ID_EMPRESA STRING,
MONTO DOUBLE,
FECHA STRING
)
USING DELTA
LOCATION 'dbfs:/mnt/lakehousedata/silver/venta/'
PARTITIONED BY (FECHA)
;

-- COMMAND ----------

DESCRIBE FORMATTED SILVER_RETAIL.VENTA_PARQUET;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(""" SET hive.exec.dynamic.partition.mode=nonstrict """)

-- COMMAND ----------

-- DBTITLE 1,8. Carga curada a la tabla "Transacción" en la capa Silver
INSERT OVERWRITE TABLE SILVER_RETAIL.VENTA_PARQUET 
SELECT 
  TRIM(ID_PERSONA) 
, TRIM(ID_EMPRESA)
, CAST(MONTO AS DOUBLE)
, FECHA
FROM BRONZE_RETAIL.VENTA;

-- COMMAND ----------

INSERT OVERWRITE TABLE SILVER_RETAIL.VENTA 
SELECT 
  TRIM(ID_PERSONA) 
, TRIM(ID_EMPRESA)
, CAST(MONTO AS DOUBLE)
, FECHA
FROM BRONZE_RETAIL.VENTA;

-- COMMAND ----------

SELECT FECHA, COUNT(1) FROM SILVER_RETAIL.VENTA
GROUP BY FECHA;
