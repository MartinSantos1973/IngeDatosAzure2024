# Databricks notebook source
# DBTITLE 1,1. Creación de tabla "Persona" asociada al directorio del archivo (RAW)
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS TMP_RETAIL CASCADE;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS TMP_RETAIL;
# MAGIC
# MAGIC DROP TABLE IF EXISTS TMP_RETAIL.CLIENTE;
# MAGIC
# MAGIC CREATE TABLE TMP_RETAIL.CLIENTE(
# MAGIC     ID STRING,
# MAGIC     NOMBRE STRING,
# MAGIC     TELEFONO STRING,
# MAGIC     CORREO STRING,
# MAGIC     FECHA_INGRESO DATE,
# MAGIC     EDAD INT,
# MAGIC     SALARIO DOUBLE,
# MAGIC     ID_EMPRESA STRING
# MAGIC )
# MAGIC --ROW FORMAT DELIMITED
# MAGIC --FIELDS TERMINATED BY '|'
# MAGIC --LINES TERMINATED BY '\n'
# MAGIC --STORED AS TEXTFILE
# MAGIC --LOCATION 'dbfs:/mnt/datalake/bronze/persona/'
# MAGIC --TBLPROPERTIES(
# MAGIC --    'skip.header.line.count'='1',
# MAGIC --   'store.charset'='ISO-8859-1', 
# MAGIC --   'retrieve.charset'='ISO-8859-1')
# MAGIC USING CSV
# MAGIC OPTIONS (path "dbfs:/mnt/lakehousedata/tmp/cliente/",
# MAGIC         delimiter "|",
# MAGIC         header "true")
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TMP_RETAIL.CLIENTE LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED TMP_RETAIL.CLIENTE;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY TMP_RETAIL.CLIENTE;

# COMMAND ----------

# DBTITLE 1,2. Creación de tabla "Persona" asociada al directorio del archivo (DELTA)
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS BRONZE_RETAIL CASCADE;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS BRONZE_RETAIL;
# MAGIC
# MAGIC DROP TABLE IF EXISTS BRONZE_RETAIL.CLIENTE;
# MAGIC
# MAGIC CREATE TABLE BRONZE_RETAIL.CLIENTE(
# MAGIC     ID STRING,
# MAGIC     NOMBRE STRING,
# MAGIC     TELEFONO STRING,
# MAGIC     CORREO STRING,
# MAGIC     FECHA_INGRESO STRING,
# MAGIC     EDAD STRING,
# MAGIC     SALARIO STRING,
# MAGIC     ID_EMPRESA STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/mnt/lakehousedata/bronze/cliente/"
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY BRONZE_RETAIL.CLIENTE;

# COMMAND ----------

# DBTITLE 1,3. Carga del dataset "Cliente" hacia un dataframe
df_cliente = spark.read.format("csv").option("header","true").option("delimiter","|").load("/mnt/lakehousedata/tmp/cliente")

df_cliente.show(2)

# COMMAND ----------

# DBTITLE 1,4. Escritura del dataframe "Cliente" hacia Delta
df_cliente.write \
      .format("delta") \
      .mode("overwrite") \
      .save("/mnt/lakehousedata/bronze/cliente")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM BRONZE_RETAIL.CLIENTE 
# MAGIC --WHERE EDAD > 50
# MAGIC ORDER BY ID DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY BRONZE_RETAIL.CLIENTE;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO BRONZE_RETAIL.CLIENTE VALUES
# MAGIC ("102", "Juan", "500", "ricardo@gmail.com","2023-02-22","40","1000","1");
# MAGIC --("lentils", "brown", 1000, true),
# MAGIC --("jelly", "rainbow", 42.5, false);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY BRONZE_RETAIL.CLIENTE;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM BRONZE_RETAIL.CLIENTE VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE BRONZE_RETAIL.CLIENTE;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY BRONZE_RETAIL.CLIENTE;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC SET spark.databricks.delta.vacuum.logging.enabled = true;
# MAGIC
# MAGIC VACUUM BRONZE_RETAIL.CLIENTE RETAIN 0 HOURS;
# MAGIC -- DRY RUN;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY BRONZE_RETAIL.CLIENTE;

# COMMAND ----------

# DBTITLE 1,5. Creación de tabla "Empresa" asociada al directorio del archivo (RAW)
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS TMP_RETAIL.EMPRESA;
# MAGIC
# MAGIC CREATE TABLE TMP_RETAIL.EMPRESA(
# MAGIC     ID STRING,
# MAGIC     NOMBRE STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (path "dbfs:/mnt/lakehousedata/tmp/empresa/",
# MAGIC         delimiter "|",
# MAGIC         header "true")
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,6. Creación de tabla "Empresa" asociada al directorio del archivo (DELTA)
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS BRONZE_RETAIL.EMPRESA;
# MAGIC
# MAGIC CREATE TABLE BRONZE_RETAIL.EMPRESA(
# MAGIC     ID STRING,
# MAGIC     NOMBRE STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/mnt/lakehousedata/bronze/empresa/"
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,7. Carga del dataset "Empresa" hacia un dataframe
df_empresa = spark.read.format("csv").option("header","true").option("delimiter","|").load("/mnt/lakehousedata/tmp/empresa")

df_empresa.show()

# COMMAND ----------

# DBTITLE 1,8. Escritura del dataframe "Empresa" hacia delta
df_empresa.write \
      .format("delta") \
      .mode("overwrite") \
      .save("/mnt/lakehousedata/bronze/empresa")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM BRONZE_RETAIL.EMPRESA;

# COMMAND ----------

# DBTITLE 1,9. Creación de tabla "Transacción" asociada al directorio del archivo (RAW)
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS TMP_RETAIL.VENTA;
# MAGIC
# MAGIC CREATE TABLE TMP_RETAIL.VENTA(
# MAGIC     ID_PERSONA STRING,
# MAGIC     ID_EMPRESA STRING,
# MAGIC     MONTO DOUBLE,
# MAGIC     FECHA STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (path "dbfs:/mnt/lakehousedata/tmp/venta/",
# MAGIC         delimiter "|",
# MAGIC         header "true")
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,10. Creación de tabla "Transacción" asociada al directorio del archivo (DELTA)
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS BRONZE_RETAIL.VENTA;
# MAGIC
# MAGIC CREATE TABLE BRONZE_RETAIL.VENTA(
# MAGIC     ID_PERSONA STRING,
# MAGIC     ID_EMPRESA STRING,
# MAGIC     MONTO STRING,
# MAGIC     FECHA STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/mnt/lakehousedata/bronze/venta/"
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,11. Carga del dataset "Venta" hacia un dataframe
df_venta = spark.read.format("csv").option("header","true").option("delimiter","|").load("/mnt/lakehousedata/tmp/venta")

df_venta.show()

# COMMAND ----------

# DBTITLE 1,12. Escritura del dataframe "Cliente" hacia Delta
df_venta.write \
      .format("delta") \
      .mode("overwrite") \
      .save("/mnt/lakehousedata/bronze/venta")

print("Iniciando")

# COMMAND ----------

# DBTITLE 1,13. Ejecución de código SQL
# MAGIC %sql
# MAGIC -- [HIVE] Lanzamos un query
# MAGIC SELECT 
# MAGIC     P.NOMBRE,
# MAGIC     P.EDAD,
# MAGIC     P.SALARIO
# MAGIC FROM 
# MAGIC     BRONZE_RETAIL.CLIENTE P
# MAGIC WHERE
# MAGIC     P.EDAD > 35 AND
# MAGIC     P.SALARIO > 5000;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- [HIVE] Lanzamos un query
# MAGIC SELECT 
# MAGIC     *
# MAGIC FROM 
# MAGIC     BRONZE_RETAIL.VENTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE BRONZE_RETAIL.VENTA
# MAGIC ZORDER BY ID_PERSONA;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC SET spark.databricks.delta.vacuum.logging.enabled = true;
# MAGIC
# MAGIC VACUUM BRONZE_RETAIL.VENTA RETAIN 0 HOURS;
# MAGIC -- DRY RUN;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY BRONZE_RETAIL.VENTA;
