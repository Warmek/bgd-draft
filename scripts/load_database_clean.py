import pandas as pd
import os
from pathlib import Path


datasets_amount = 4

for year in os.listdir("data/"):
    Path(f"buff/cleaned_csv/").mkdir(parents=True, exist_ok=True)
    for month_parquet in os.listdir(f"data/{year}/"):
        if(datasets_amount<=0):
            break
        df = pd.read_parquet(f"data/{year}/{month_parquet}", engine="pyarrow")
        # Drop rows with nulls (about 24.6%)
        df = df.dropna(how='any', axis=0)
        mappings = {
            'Y': True,
            'N': False
        }
        df['shared_request_flag'] = df['shared_request_flag'].map(lambda s: mappings[s])
        df['shared_match_flag'] = df['shared_match_flag'].map(lambda s: mappings[s])
        df['access_a_ride_flag'] = df['access_a_ride_flag'].map(lambda s: mappings[s])
        df['wav_request_flag'] = df['wav_request_flag'].map(lambda s: mappings[s])
        df['wav_match_flag'] = df['wav_match_flag'].map(lambda s: mappings[s])
        print(df.head())
        print(f'Save {month_parquet} to csv')
        df.to_csv(f"buff/cleaned_csv/{month_parquet}_cleared.csv")
        datasets_amount = datasets_amount - 1
