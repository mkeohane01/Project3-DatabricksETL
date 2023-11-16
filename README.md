# Project3-DatabricksETL: Dow Jones Industrial Average Data Analysis

[![Run Databricks Workflow](https://github.com/mkeohane01/Project3-DatabricksETL/actions/workflows/databricks_workflow.yml/badge.svg)](https://github.com/mkeohane01/Project3-DatabricksETL/actions/workflows/databricks_workflow.yml)

[![Continuous Integration](https://github.com/mkeohane01/Project3-DatabricksETL/actions/workflows/continuous_integration.yml/badge.svg)](https://github.com/mkeohane01/Project3-DatabricksETL/actions/workflows/continuous_integration.yml)

## Introduction

This project is dedicated to performing data analysis on the Dow Jones Industrial Average (DJIA) stock prices utlizing Databricks. Through Databricks notebooks, I was able to perform Extract Transform Load (ETL) operations using Delta Lake data lakehouse storage. Delta Lake is very helpful because of the metadata layer which gives access to ACID properites (Atomicity, Consistency, Isolation, and Durability) and revisioning history that traditional Data Lakes dont support.

## Features

- **Data Extraction**: Extracting DJIA stock data (both real and nominal) over last 100 years from csv and saving in a Delta Lake database.
    - ETL-notebooks/Extract
- **Data Transformation**: Transforming the data using Spark SQL to get yearly data as well as finding ratio of real and nominal values.
    - ETL-notebooks/TransformLoad
- **Data Loading**: Storing the transformed data in a new table in the Delta Lake database.
    - ETL-notebooks/TransformLoad
- **Data Analysis/Visualization**: Loaded and Visualized the transformed data for analysis
    - ETL-notebooks/QueryVisualize

## Workflow Useage

- The ETL workflow job is ran automatically through calling Databricks API
    - Manually run using:
    ```bash 
    make run-workflow
    ```
    - The job flow runs 3 files from the ETL-notebooks folder on the Databricks cluster: 
        - Extract -> TransformLoad -> QueryVisualize
- Project dependencies can be found in requirements.txt

## Visualizations


## Recommendations to Management

Based on our analysis of the DJIA stock prices, we recommend the following to the management team:

1. **Safe Long Investment**: Stock price shows consistent growth over a long period of time for safe long term investment year by year.
3. **Inflation Analysis**: We can also look at the real-to-nominal stock price ratio to analyze inflation. A higher real-to-nominal stock price ratio indicates that inflation has been low, preserving the purchasing power of stock investments, whereas a lower ratio suggests higher inflation, eroding the real value and purchasing power of these investments. In this case it is relatively steady but we can see that the purchasing power has generally decreased over time.

In conclusion, I recommending heavily investing in Dow Jones Industral Average to have a very save investment with clear returns which will fight inflation more than a bank or other savings.
