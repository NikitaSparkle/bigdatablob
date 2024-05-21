import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("BigDataLab").getOrCreate()

# Step 2: Read the JSON file from Blob Storage
df = spark.read.json("/dbfs/user/hive/warehouse/download.json")

# Step 3: Add a new column with random dates
random_dates = pd.date_range(start="2020-01-01", end="2023-12-31", periods=df.count())
df = df.withColumn("DateRegistration", lit(np.random.choice(random_dates, df.count()).astype(str)))

# Step 4: Save the DataFrame to Databricks SQL Warehouse
jdbc_url = "jdbc:spark://mydatabricksworkspace.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=/sql/1.0/endpoints/abcdef1234567890;AuthMech=3;UID=token;PWD=dapi1234567890abcdef1234567890abcdef"

df.write.format("jdbc").option("url", jdbc_url).option("dbtable", "todos_data").option("user", "token").option("password", "dapi1234567890abcdef1234567890abcdef").save()
