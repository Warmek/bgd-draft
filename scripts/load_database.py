import pandas as pd
import os
from pathlib import Path


user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT", 5432)
database = "bgd"

for year in os.listdir("data/"):
    Path(f"buff/csv/").mkdir(parents=True, exist_ok=True)
    for month_parquet in os.listdir(f"data/{year}/"):
        df = pd.read_parquet(f"data/{year}/{month_parquet}", engine="pyarrow")
        print(f'Save {month_parquet} to csv')
        df.to_csv(f"buff/csv/{month_parquet}.csv")

