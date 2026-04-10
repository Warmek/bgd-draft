-- Aggregate metrics based on start and end locations
CREATE VIEW gold_route_performance_summary AS
SELECT
    originating_base_num AS origin_base,
    "PULocationID" AS origin_location,
    "DOLocationID" AS destination_location,
    COUNT(index) AS total_trips_on_route,
    SUM(base_passenger_fare) AS total_revenue_route,
    SUM(trip_miles) AS total_miles_on_route,
    CAST(SUM(base_passenger_fare) AS NUMERIC(12, 2)) / COUNT(index) AS avg_revenue_per_trip
FROM
    public.trips_cleaned
GROUP BY
    originating_base_num,
    "PULocationID",
    "DOLocationID"
ORDER BY
    total_revenue_route DESC;

SELECT * FROM gold_route_performance_summary LIMIT 100;
