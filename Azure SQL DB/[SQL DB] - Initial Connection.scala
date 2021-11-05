// Databricks notebook source
// MAGIC %md
// MAGIC ####Option 1: Connect to SQL Database
// MAGIC #####Using JDBC
// MAGIC Nota: Databricks Runtime contains JDBC drivers for SQL Server and Azure SQL Database

// COMMAND ----------

val jdbcHostname = "<servername>.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "NombreBBDD"
val jdbcUsername = "usuario"
val jdbcPassword = "contraseña"


// COMMAND ----------

//Check that JDBC driver is available

Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

displayHTML("- JDBC driver is available")

// COMMAND ----------

//Create the JDBC URL

// Create the JDBC URL without passing in the user and password parameters.
var jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")

displayHTML("- JDBC URL was created")

// COMMAND ----------

//Check connectivity to the SQLServer database

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)

displayHTML("- Check connectivity to the SQLServer database")
