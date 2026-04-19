import os
from pathlib import Path
from pyspark.sql import SparkSession


MAX_MEMORY = "15g"

spark = SparkSession \
    .builder \
    .appName("Convert Data to csv") \
    .config("spark.executor.memory", MAX_MEMORY) \
    .config("spark.driver.memory", MAX_MEMORY) \
    .getOrCreate()

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT", 5432)
database = "bgd"

for year in os.listdir("data/"):
    if year=='other':
        break;
    Path(f"buff/csv/").mkdir(parents=True, exist_ok=True)
    for month_parquet in os.listdir(f"data/{year}/"):
        df = spark.read.parquet(f"data/{year}/{month_parquet}").cache()
        print(f'Save {month_parquet} to csv')
        df.write.csv(f"buff/csv/{month_parquet}.csv", mode='overwrite')
        df.unpersist()

spark.stop()
