CREATE DATABASE FLIGHTS;

CREATE SCHEMA FLIGHTS.KPI;

CREATE TABLE STG_FLIGHTS_KPI (
    window_start    TIMESTAMP,
    origin_country  TEXT,
    total_flights   INT,
    avg_velocity    FLOAT,
    on_ground       INT
);

CREATE TABLE FLIGHTS_KPI (
    window_start    TIMESTAMP,
    origin_country  TEXT,
    total_flights   INT,
    avg_velocity    FLOAT,
    on_ground       INT,
    load_time       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, origin_country)
);