# Databricks notebook source
# MAGIC %md
# MAGIC ###Data Quality Monitoring & Anomaly Detection Project

# COMMAND ----------

import requests
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Define variables

# COMMAND ----------

API_KEY = "88b1862508f7780614e4ca6542ef0d32"
CITIES = ["Johannesburg","Cape Town","Durban","London","Hong Kong","Rome","Amsterdam","San Francisco","Paris","New York","Sydney","Barcelona","Berlin","Seoul","Kuala Lumpur","Dubai","Prague","Tokyo","Instanbul","Shanghai","Los Angeles","Madrid","Toronto","Chicago","Vienna","Seattle","Milan","Boston","Vancouver","Miami"]
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITIES}&appid={API_KEY}&units=metric"
records = []

# COMMAND ----------

# MAGIC %md
# MAGIC Fetch the data from the API

# COMMAND ----------

for CITY in CITIES:
    URL = f"http://api.openweathermap.org/data/2.5/forecast?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(URL)

    if response.status_code == 200:
        weather_data = response.json()
        if "list" in weather_data and weather_data["list"]:
            for entry in weather_data["list"]:
                record = {
                    "city": CITY,
                    "timestamp": entry["dt_txt"],
                    "temperature": entry["main"]["temp"],"humidity": entry["main"]["humidity"],"pressure": entry["main"]["pressure"],"wind_speed": entry["wind"]["speed"],"weather": entry["weather"][0]["description"]
                    }
                records.append(record)
        else: 
            print(f"No forecast data found for {CITY}")
    else:
        print(f"Failed to fetch data for {CITY}: {response.status_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC Create a schema and Dataframe from all records

# COMMAND ----------

schema = StructType([
    StructField("city",StringType(),True),
    StructField("timestamp",StringType(),True),
    StructField("temperature",StringType(),True),
    StructField("humidity",IntegerType(),True),
    StructField("pressure",IntegerType(),True),
    StructField("wind_speed",StringType(),True),
    StructField("weather",StringType(),True),
])

df_bronze = spark.createDataFrame(records,schema=schema)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Connect and transfer Bronze Layer data to PostgreSQL

# COMMAND ----------

jdbc_url_bronze = "jdbc:postgresql://ep-yellow-haze-a8suj96t-pooler.eastus2.azure.neon.tech:5432/Data_Quality_Monitoring_Anomaly_DetectionDB?sslmode=require"

df_bronze.write \
    .format("jdbc")\
        .option("url",jdbc_url_bronze)\
            .option("dbtable","WeatherDataBronze")\
                .option("user","neondb_owner")\
                    .option("password","npg_gIjCOkvw9t5h")\
                        .option("driver","org.postgresql.Driver")\
                            .mode("overwrite")\
                                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Cleaning (Silver Layer)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Convert timestamp string to Spark TimestampType

# COMMAND ----------

df_silver = df.withColumn("timestamp", to_timestamp("timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC For the City ID

# COMMAND ----------

from pyspark.sql.functions import dense_rank
from pyspark.sql.window import Window

window_spec = Window.orderBy("city")
df_silver = df_silver.withColumn("city_id", dense_rank().over(window_spec))

# COMMAND ----------

# MAGIC %md
# MAGIC Add derived fields

# COMMAND ----------

df_silver = df_silver.withColumn("month",month(col("timestamp")))\
    .withColumn("year",year("timestamp"))\
        .withColumn("wind_speed_kmh", col("wind_speed") * 3.6) \
            .drop("wind_speed") \
                .withColumnRenamed("wind_speed_kmh","wind_speed")\
                    .withColumn("row_id", monotonically_increasing_id()) #Unique ID

# COMMAND ----------

# MAGIC %md
# MAGIC Preview the cleaned Dataframe

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Connect and transfer Silver Layer data to PostgreSQL

# COMMAND ----------

 jdbc_url_silver = "jdbc:postgresql://ep-yellow-haze-a8suj96t-pooler.eastus2.azure.neon.tech:5432/Data_Quality_Monitoring_Anomaly_DetectionDB?sslmode=require"

df_silver.write\
    .format("jdbc")\
        .option("url", jdbc_url_silver)\
            .option("dbtable","WeatherDataSilver")\
                .option("user","neondb_owner")\
                    .option("password","npg_gIjCOkvw9t5h")\
                        .option("driver","org.postgresql.Driver")\
                            .mode("overwrite")\
                                .save()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregate data by city and day (Gold Layer)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, mean, stddev
from pyspark.sql.functions import abs


# COMMAND ----------

# MAGIC %md
# MAGIC Aggregate data by City and Day of the week
# MAGIC Find the avg for the attributes

# COMMAND ----------


df_gold = df_silver.groupBy("city","year","month").agg(
    mean("temperature").alias("avg_temperature"),
    mean("humidity").alias("avg_humidity"),
    mean("pressure").alias("avg_pressure"),
    mean("wind_speed").alias("avg_wind_speed"),
    stddev("temperature").alias("stddev_temperature")
)


# COMMAND ----------

# MAGIC %md
# MAGIC Detect anomalies in temperature using z-score

# COMMAND ----------

window_spec = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df_gold = df_gold.withColumn(
    "z_score_temperature",
    (col("avg_temperature") - mean("avg_temperature").over(window_spec)) / 
    stddev("avg_temperature").over(window_spec)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Flag anomalies

# COMMAND ----------

df_gold = df_gold.withColumn(
    "is_anomaly_zscore", abs(col("z_score_temperature")) > 2)\
        .withColumn("is_anomaly_rule_based", (col("avg_temperature") > 40) | (col("avg_temperature") < -10))\
            .withColumn("final_anomaly_flag", col("is_anomaly_zscore") | col("is_anomaly_rule_based")
                        )

# COMMAND ----------

df_gold.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connect and transfer data to PostgreSQL

# COMMAND ----------

jdbc_url = "jdbc:postgresql://ep-yellow-haze-a8suj96t-pooler.eastus2.azure.neon.tech:5432/Data_Quality_Monitoring_Anomaly_DetectionDB?sslmode=require"

df_gold.write \
    .format("jdbc") \
        .option("url", jdbc_url) \
            .option("dbtable", "WeatherDataGold") \
                .option("user", "neondb_owner") \
                    .option("password", "npg_gIjCOkvw9t5h") \
                        .option("driver", "org.postgresql.Driver") \
                            .mode("overwrite") \
                                .save()
