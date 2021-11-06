// Databricks notebook source
dbutils.widgets.text("TABLE_NAME","")
dbutils.widgets.text("SCHEMA_NAME","")
dbutils.widgets.text("COLUMN_NAME","") 

val p_schema_name = dbutils.widgets.get("SCHEMA_NAME")
val p_table_name = dbutils.widgets.get("TABLE_NAME")
val p_column_name = dbutils.widgets.get("COLUMN_NAME")

// COMMAND ----------

// MAGIC %run "./_Initial Connection to SQL DB"

// COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS DELTA_LEGO")
spark.sql("USE DELTA_LEGO;")

// COMMAND ----------

// Define the input and output formats and paths and the table name.
val v_write_format = "delta"
val v_save_path = "/mnt/bronze/" + p_table_name

// Load the data from its source.
val source_table_sql = spark
  .read
  .jdbc(jdbcUrl, p_schema_name + "." + p_table_name, connectionProperties)

// Write the data to its target.
source_table_sql
        .write
        .format(v_write_format)
        .mode("append")
        .save(v_save_path)

// Create the table.
spark.sql("CREATE TABLE " + p_table_name + " USING DELTA LOCATION '" + v_save_path + "'")

// COMMAND ----------

val delta_bronze_table = spark.table(p_table_name)

display(delta_bronze_table)
