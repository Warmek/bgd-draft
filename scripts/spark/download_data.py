from pathlib import Path
import requests
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Download data").getOrCreate()

years = ["2025", "2024"]

for year in years:
    for month in range(1,13):
        if month<10:
            month="0"+str(month)
        month=str(month)
        url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year}-{month}.parquet"
        print(f"Downloading data for hire {year}-{month}")
        print(url)
        Path(f"data/{year}/").mkdir(parents=True, exist_ok=True)
        r = requests.get(url)
        with open(f'data/{year}/for_hire_tripdata_{year}-{month}.parquet', 'wb') as outfile:
            outfile.write(r.content)


spark.stop()

