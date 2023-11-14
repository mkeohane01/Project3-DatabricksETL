# Databricks notebook source
# Extract the data from stock_data
# I uploaded the data manully to data bricks file system
table_name = "stock_data"
table_path = "hive_metastore.default.stock_data"
data = spark.table(table_path)

# Show the first 10 rows of the table
data.show(10)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Transform using SQL and then Load in to Delta Lake table yearlystocks_delta
# MAGIC -- Grouping the data by year and averaging the monthly stock prices
# MAGIC CREATE TABLE
# MAGIC yearlystocks_delta
# MAGIC USING DELTA
# MAGIC SELECT 
# MAGIC   year(date) AS year, 
# MAGIC   ROUND(AVG(real),2) AS avg_real, 
# MAGIC   ROUND(AVG(nominal),2) AS avg_nominal
# MAGIC FROM 
# MAGIC   stock_data 
# MAGIC GROUP BY 
# MAGIC   year(date)
# MAGIC ORDER BY
# MAGIC   year
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `hive_metastore`; select * from `default`.`yearlystocks_delta` limit 100;
