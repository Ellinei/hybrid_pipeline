-- Create separate databases for app and Airflow
CREATE DATABASE airflow;

-- Enable TimescaleDB on the trading database
\c trading
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Placeholder schemas (tables come in later phases)
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS features;
CREATE SCHEMA IF NOT EXISTS trading;
