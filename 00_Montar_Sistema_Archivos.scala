// Databricks notebook source
// DBTITLE 1,0. Configuración de parámetros, parte superior
dbutils.widgets.text("PRM_STORAGE_ACCOUNT_NAME","")
dbutils.widgets.text("PRM_CONTAINER_NAME","")
val PRM_STORAGE_ACCOUNT_NAME = dbutils.widgets.get("PRM_STORAGE_ACCOUNT_NAME")
val PRM_CONTAINER_NAME = dbutils.widgets.get("PRM_CONTAINER_NAME")

// COMMAND ----------

// DBTITLE 1,1. Credenciales de acceso
var storage = PRM_STORAGE_ACCOUNT_NAME
var container = PRM_CONTAINER_NAME
var tokenSas = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-12-08T08:17:01Z&st=2023-12-08T00:17:01Z&spr=https&sig=PEKhmsvOlsf6GFdSPcXGbPn6DxU%2FptetlTULQcWqdII%3D"

// COMMAND ----------

// DBTITLE 1,2. Acceso a Azure
//Proceso montado al sistema de archivos de databricks
var config = "fs.azure.sas." + container+ "." + storage + ".blob.core.windows.net" 
//var config = "fs.azure.sas.datalake.dscloudtest.blob.core.windows.net" 

try{
  dbutils.fs.ls("/mnt/"+container)}
catch{
//except Exception as e:
  case e: Exception => 
  //println("Error")
  dbutils.fs.mount(
    source = "wasbs://"+container+"@"+storage+".blob.core.windows.net", 
    mountPoint = "/mnt/"+container, 
    extraConfigs = Map(config -> tokenSas))
}

// COMMAND ----------

// DBTITLE 1,3. Lectura de datos desde Azure
var df = spark.read.format("csv").option("header","true").option("delimiter", "|").load("/mnt/lakehousedata/tmp/empresa")
df.show()

// COMMAND ----------

// MAGIC %md Explorar ruta montada
// MAGIC

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /mnt/lakehousedata/tmp/cliente/

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /

// COMMAND ----------

//Proceso desmontado de archivos
//dbutils.fs.unmount("/mnt/"+container)
