import pandas as pd
import os
from pathlib import Path



for year in os.listdir("data/"):
    Path(f"buff/csv/").mkdir(parents=True, exist_ok=True)
    for month_parquet in os.listdir(f"data/{year}/"):
        df = pd.read_parquet(f"data/{year}/{month_parquet}", engine="pyarrow")
        print(len(df))
        print("----==== Null count ====----")
        print(df.isnull().sum())

# Load to database
# load_dotenv()
#
# user = os.getenv("POSTGRES_USER")
# password = os.getenv("POSTGRES_PASSWORD")
# host = os.getenv("POSTGRES_HOST")
# port = os.getenv("POSTGRES_PORT", 5432)
# database = "bgd"
# print(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
#
# engine = create_engine(
#         f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
#     )
#
#
# with pc.connect(dbname=database, user=user, password=password, host=host, port=port) as conn:
#     with conn.cursor() as cursor:
#
#         for year in os.listdir("data/"):
#             for month_parquet in os.listdir(f"data/{year}/"):
#                 df = pd.read_parquet(f"data/{year}/{month_parquet}", engine="pyarrow")
#                 size = df.size
#                 n=100_000
#                 list_df = [df[i:i+n] for i in range(0,len(df),n)]
#
#                 for chunk_df in list_df:
#                     chunk_df.to_sql(name='trips', con=engine, if_exists='append', method='multi')
#                     print(f"Inserted {n}/{size} - {(n/size)*100}")
#
#                     conn.commit()
#
#         conn.close()
