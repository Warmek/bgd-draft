show datestyle;
set datestyle to 'ISO, YMD';

COPY trips FROM '/downloads/for_hire_tripdata_2025-01.parquet.csv' DELIMITER ',' CSV HEADER;
COPY trips FROM '/downloads/for_hire_tripdata_2025-02.parquet.csv' DELIMITER ',' CSV HEADER;
COPY trips FROM '/downloads/for_hire_tripdata_2025-03.parquet.csv' DELIMITER ',' CSV HEADER;
COPY trips FROM '/downloads/for_hire_tripdata_2025-04.parquet.csv' DELIMITER ',' CSV HEADER;

COPY trips_cleaned FROM '/downloads/for_hire_tripdata_2025-01.parquet_cleared.csv' DELIMITER ',' CSV HEADER;
COPY trips_cleaned FROM '/downloads/for_hire_tripdata_2025-02.parquet_cleared.csv' DELIMITER ',' CSV HEADER;
COPY trips_cleaned FROM '/downloads/for_hire_tripdata_2025-03.parquet_cleared.csv' DELIMITER ',' CSV HEADER;
COPY trips_cleaned FROM '/downloads/for_hire_tripdata_2025-04.parquet_cleared.csv' DELIMITER ',' CSV HEADER;

SELECT trips."index",pickup."Zone",dropoff."Zone",trips.tips FROM trips_cleaned as trips JOIN zones as pickup ON trips."PULocationID"=pickup."LocationID" JOIN zones as dropoff ON trips."DOLocationID"=dropoff."LocationID" LIMIT 10;
