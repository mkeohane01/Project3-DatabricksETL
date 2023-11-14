# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# initialize spark session
spark = SparkSession.builder \
        .appName("ETL Project 3") \
        .getOrCreate()

# COMMAND ----------

# Load table of stock data
db = "stocks_database"
table = "djia_stock"

# Load the table with spark
stock_df = spark.read.table(f"{db}.{table}")

# COMMAND ----------

# Create a temporary view of the dataframe
stock_df.createOrReplaceTempView("stock_data")

# Query the view using Spark SQL and transform to keep the average real price for each year
yearly_df = spark.sql("SELECT YEAR(date) AS year, \
                        ROUND(AVG(real), 2) AS avg_real \
                        FROM stock_data \
                        GROUP BY YEAR(date) \
                        ORDER BY year")

# Show the result
yearly_df.show(5)

# COMMAND ----------

# Load this data in to new delta table yearly_djia
yearly_df.write.format("delta").mode("overwrite").saveAsTable(f"{db}.yearly_djia")

# COMMAND ----------

# Transform the data to find the ratio between real and nominal stock prices
# compare the real and nominal values each year
# create a new DataFrame that includes the ratio of real to prices and rounded to two decimal places
price_ratio = (
    stock_df
    .select("date", round((col("real") / col("nominal")), 2).alias("real_to_nominal_ratio"))
)

# show the results
price_ratio.show(5)

# COMMAND ----------

# Load this data in to new delta table djia_ratio
price_ratio.write.format("delta").mode("overwrite").saveAsTable(f"{db}.djia_ratio")
