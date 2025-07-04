# Databricks notebook source
import requests
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *

API_KEY = "88b1862508f7780614e4ca6542ef0d32"
CITY = "Johannesburg"
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"



# COMMAND ----------

#Fetch the data from the API
response = requests.get(URL)
if response.status_code == 200:
    weather_data = response.json()
else:
    raise Exception(f"API request failed with status {response.status_code}")

# COMMAND ----------

record = {
    "city": CITY,
    "timestamp": datetime.utcfromtimestamp(weather_data["dt"]).isoformat(),
    "temperature": weather_data["main"]["temp"],
    "humidity": weather_data["main"]["humidity"],
    "pressure": weather_data["main"]["pressure"],
    "wind_speed": weather_data["wind"]["speed"],
    "weather": weather_data["weather"][0]["description"]
}

# COMMAND ----------

#Define Schema
schema = StructType([
    StructField("city", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("pressure", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("weather", StringType(), True)
])
df = spark.createDataFrame([record],schema=schema)

# COMMAND ----------

#Write to Delta Lake Bronze table
bronze_path = "/mnt/datalake/weather/bronze"

df.write.format("delta").mode("append").save(bronze_path)

# COMMAND ----------

#Create a table for SQL access
spark.sql(f""" 
CREATE TABLE IF NOT EXISTS weather_bronze
    USING DELTA
    LOCATION '{bronze_path}'
""")

# COMMAND ----------

display(df)
