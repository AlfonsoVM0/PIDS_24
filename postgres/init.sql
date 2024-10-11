CREATE DATABASE rasa;
CREATE DATABASE taxi_data;

\c taxi_data;

CREATE TABLE IF NOT EXISTS raw_data (
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    RatecodeID INT,
    store_and_fwd_flag CHAR(1),
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT
);

CREATE TABLE IF NOT EXISTS analysis_results (
    analysis_date DATE,
    avg_trip_duration FLOAT,
    avg_trip_distance FLOAT,
    max_passengers INT,
    total_revenue FLOAT,
    avg_tip_amount FLOAT,
    most_common_payment_type INT,
    avg_tolls_per_trip FLOAT,
    most_frequent_driver INT,
    avg_passenger_count FLOAT,
    longest_trip FLOAT,
    highest_tip FLOAT,
    revenue_per_mile FLOAT
);


GRANT ALL PRIVILEGES ON TABLE analysis_results TO airflow;