import os
from pathlib import Path
from pyspark.sql import SparkSession

MAX_MEMORY = "15g"

spark = SparkSession \
    .builder \
    .appName("Clean Data") \
    .config("spark.executor.memory", MAX_MEMORY) \
    .config("spark.driver.memory", MAX_MEMORY) \
    .getOrCreate()



mappings = {
    'Y': 'True',
    'N': 'False'
}

for year in os.listdir("data/"):
    Path(f"buff/cleaned_csv/").mkdir(parents=True, exist_ok=True)
    for month_parquet in os.listdir(f"data/{year}/"):
        df = spark.read.parquet(f"data/{year}/{month_parquet}").cache()
        # Drop rows with nulls (about 24.6%)
        df = df.dropna(how='any')

        df = df.replace('Y', 'True')
        df = df.replace('N', 'True')

        df = df.withColumn('shared_request_flag', df['shared_request_flag'].cast("boolean"))
        df = df.withColumn('shared_match_flag', df['shared_match_flag'].cast("boolean"))
        df = df.withColumn('access_a_ride_flag', df['access_a_ride_flag'].cast("boolean"))
        df = df.withColumn('wav_request_flag', df['wav_request_flag'].cast("boolean"))
        df = df.withColumn('wav_match_flag', df['wav_match_flag'].cast("boolean"))

        print(df.head())
        print(f'Save {month_parquet} to csv')
        df.write.csv(f"buff/cleaned_csv/{month_parquet}_cleared.csv", mode='overwrite')
        df.unpersist()

spark.stop()
