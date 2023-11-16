# Databricks notebook source
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas as pd

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

# Convert to pandas for plotting
monthly_stock_pd = monthly_stock_df.toPandas()
yearly_stock_pd = yearly_stock_df.toPandas()
ratio_stock_pd = ratio_stock_df.toPandas()

fig1, ax1 = plt.subplots()
ax1.plot(yearly_stock_pd['year'], yearly_stock_pd['avg_real'])

ax1.set(xlabel='Year', ylabel='Stock Price',
       title='Yearly Dow Jones Stock Price')
ax1.grid()

fig2, ax2 = plt.subplots()
ax2.plot(ratio_stock_pd['date'], ratio_stock_pd['real_to_nominal_ratio'])

ax2.set(xlabel='Date', ylabel='Real:Nominal Stock Price Ratio',
       title='Dow Jones Stock- Real:Nominal Ratio')
ax2.grid()

# fig1.savefig('../figs/yearlystockprice.png')
# fig2.savefig('../figs/stockratio.png')

# COMMAND ----------

# Visualize the data

# # plot monthly stocks
# display(monthly_stock_df.select('date', 'real').orderBy('date'), mode='matplotlib', 
#         title='Monthly DJIA Stock Prices', xlabel='Date', ylabel='Evaluation')

# # plot yearly stocks
# display(yearly_stock_df.select('year', 'avg_real').orderBy('year'), mode='matplotlib', 
#         title='Yearly DJIA Stock Prices', xlabel='Year', ylabel='Evaluation')

# # plot the ratio
# display(ratio_stock_df.select('date', 'real_to_nominal_ratio').orderBy('date'), mode='matplotlib', 
#         title='Ratio of Real:Nominal', xlabel='Date', ylabel='Ratio')
