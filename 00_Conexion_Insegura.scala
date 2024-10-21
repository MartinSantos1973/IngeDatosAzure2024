// Databricks notebook source
// Configurar las variables de conexion
val storageAccountName = "adlsesping09hseu02desa02"
val containerName = "lakehousedata"
val accessKey = "81jvqu/+q7CtBJEiyyDVjPOAu/taNo+y+e4koTXj6ADOWVnSSPfB/3GF/39ZrRkEttQLwbHzZGsW+ASt4PO9hw=="

// COMMAND ----------

// Configurar las acciones de spark para acceder al almacenamiento
spark.conf.set(s"fs.azure.account.key.$storageAccountName.blob.core.windows.net", accessKey)

// COMMAND ----------

dbutils.fs.ls("wasbs://lakehousedata@adlsesping09hseu02desa02.dfs.core.windows.net/" )

// COMMAND ----------

// Leer datos desde el almacenamiento
val filePath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/tmp/cliente/persona.csv"
//val filepath = s"abfss://$containerName@$storageAccountName.blob.core.windows.net/tmp/cliente/persona.csv"

val df_cliente = spark.read.format("csv").option("header","true").option("delimiter","|").load(filePath)

// COMMAND ----------

// mostrar dataframe
//display(df_cliente)
df_cliente.show()
