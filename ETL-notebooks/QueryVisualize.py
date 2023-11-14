# Databricks notebook source
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import col

# initialize spark session
spark = SparkSession.builder \
        .appName("ETL Project 3") \
        .getOrCreate()


# COMMAND ----------

# load all data tables
db = "stocks_database"
monthly_table = "djia_stock"
yearly_table = "yearly_djia"
ratio_table = "djia_ratio"

# Load the tables with spark
monthly_stock_df = spark.read.table(f"{db}.{monthly_table}")
yearly_stock_df = spark.read.table(f"{db}.{yearly_table}")
ratio_stock_df = spark.read.table(f"{db}.{ratio_table}")

# COMMAND ----------

# Visualize the data

# plot monthly stocks
display(monthly_stock_df.select('date', 'real').orderBy('date'), mode='matplotlib', 
        title='Monthly DJIA Stock Prices', xlabel='Date', ylabel='Evaluation')

# plot yearly stocks
display(yearly_stock_df.select('year', 'avg_real').orderBy('year'), mode='matplotlib', 
        title='Yearly DJIA Stock Prices', xlabel='Year', ylabel='Evaluation')

# COMMAND ----------

# plot the ratio
display(ratio_stock_df.select('date', 'real_to_nominal_ratio').orderBy('date'), mode='matplotlib', 
        title='Ratio of Real:Nominal', xlabel='Date', ylabel='Ratio')
