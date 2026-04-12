import os
from dotenv import load_dotenv
import psycopg2 as pc
from pyspark.sql import SparkSession

MAX_MEMORY = "15g"

spark = SparkSession \
    .builder \
    .appName("Insert into database") \
    .config("spark.executor.memory", MAX_MEMORY) \
    .config("spark.driver.memory", MAX_MEMORY) \
    .getOrCreate()

load_dotenv()

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT", 5432)
database = "bgd"



with pc.connect(dbname=database, user=user, password=password, host=host, port=port) as conn:
    with conn.cursor() as cursor:
        df = spark.read.parquet(f"data/taxi_zone_lookup.csv").cache()
        df.write.jdbc(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}", "zones")


