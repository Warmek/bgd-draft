import pandas as pd
import os
from dotenv import load_dotenv, dotenv_values
import psycopg2 as pc
from sqlalchemy import create_engine
from pathlib import Path

load_dotenv()

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT", 5432)
database = "bgd"

print(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )

with pc.connect(dbname=database, user=user, password=password, host=host, port=port) as conn:
    with conn.cursor() as cursor:
        df = pd.read_csv(f"data/taxi_zone_lookup.csv")
        df.to_sql(name='zones', con=engine, if_exists='append', method='multi')

        conn.commit()

        conn.close()
