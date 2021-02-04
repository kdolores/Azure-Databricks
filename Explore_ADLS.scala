// Databricks notebook source
// MAGIC %md
// MAGIC ### Analizar datos de archivo CSV en ADLS Gen2
// MAGIC 
// MAGIC Este es un Notebook que te muestra como establecer conexión con datos que residen en un archivo CSV en un contenedor de Azure Data Lake Storage Gen2

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1. Creación de un sistema de archivos en la cuenta de Azure Data Lake Storage Gen2
// MAGIC 
// MAGIC Vamos a relacionar el Data Lake con el Spark clúster usando una ID de aplicación y montarlo como una unidad local.

// COMMAND ----------

val storageAccountName = ""
val appID = ""
val secret = ""
val fileSystemName = ""
val tenantID = ""

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> appID,
  "fs.azure.account.oauth2.client.secret" -> secret,
  "fs.azure.account.oauth2.client.endpoint" -> ("https://login.microsoftonline.com/" + tenantID + "/oauth2/token"),
  "fs.azure.createRemoteFileSystemDuringInitialization" -> "true")

dbutils.fs.mount(
  source = "abfss://" + fileSystemName + "@" + storageAccountName + ".dfs.core.windows.net/",
  mountPoint = "/mnt/data",
  extraConfigs = configs)

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2. Validamos y leemos un archivo dentro del directorio montado

// COMMAND ----------

val df = spark.read.option("header",true).csv("/mnt/data/DirectorioEnADLS/ArchivoALeer.csv")

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3. Seleccionamos algunas columnas y generamos un nuevo archivo en el Data Lake

// COMMAND ----------

val selected = df.select("NombreColumna1","NombreColumna2")

// COMMAND ----------

df.write.csv("/mnt/data/DirectorioEnADLS/ArchivoNuevo.csv")
