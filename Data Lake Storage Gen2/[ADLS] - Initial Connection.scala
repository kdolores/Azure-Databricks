// Databricks notebook source
// MAGIC %md
// MAGIC ####Create connection to ADLS Gen2

// COMMAND ----------

// MAGIC %md
// MAGIC *NOTA: Primero crear el secret scope y asociarlo al secret-value de la app en azure*

// COMMAND ----------

// MAGIC %md
// MAGIC *Validación inicial:*

// COMMAND ----------

val mysecret = dbutils.secrets.get("SecretBucket","secret_databricks")
mysecret.foreach{ println }

// COMMAND ----------

val clientID = "51c2ae02-f3ed-49a7-*****-**********"
val tenantID = "5701ab04-a93c-4c89-*****-**********"
val ClientSecret = "secret_databricks" //nombre del secret en la app
val scopeName = "SecretBucket" //creado en el cli


// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> clientID,
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope=scopeName,key=ClientSecret),
  "fs.azure.account.oauth2.client.endpoint" -> ("https://login.microsoftonline.com/" + tenantID + "/oauth2/token")
)


// COMMAND ----------

// Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container>@<nombre-storage>.dfs.core.windows.net/",
  mountPoint = "/mnt/<nombre>",
  extraConfigs = configs
)

// COMMAND ----------

// MAGIC %fs ls dbfs:/mnt

// COMMAND ----------

// MAGIC %md
// MAGIC ######Plan B para conexión al ADLS

// COMMAND ----------

/*
spark.conf.set("fs.azure.account.auth.type.saanalyticsdedev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.saanalyticsdedev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.saanalyticsdedev.dfs.core.windows.net", clientID)
spark.conf.set("fs.azure.account.oauth2.client.secret.saanalyticsdedev.dfs.core.windows.net",secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.saanalyticsdedev.dfs.core.windows.net", "https://login.microsoftonline.com/4cb67550-932f-4e31-92c9-41c3c69d0131/oauth2/token")
*/
