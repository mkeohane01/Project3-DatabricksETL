# Databricks notebook source
from pyspark.sql import SparkSession
import urllib.request

# Download the csv from GitHub and save it to DBFS
urllib.request.urlretrieve("https://raw.githubusercontent.com/mkeohane01/Project3-DatabricksETL/main/data/stock_data_DJIA.csv", "/dbfs/tmp/stock_data_DJIA.csv")

# initialize spark session
spark = SparkSession.builder \
        .appName("ETL Project 3") \
        .getOrCreate()

# COMMAND ----------

# Read in the data from the csv
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true")  \
    .option("delimiter", ",") \
    .load("/tmp/stock_data_DJIA.csv")

# COMMAND ----------

# Create New Database
spark.sql("CREATE DATABASE IF NOT EXISTS stocks_database")

# write df to table
df.write.format("delta").mode("overwrite").saveAsTable("stocks_database.DJIA_stock")
