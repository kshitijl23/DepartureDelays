CREATE DATABASE IF NOT EXISTS flightdb;
USE flightdb;

CREATE TABLE IF NOT EXISTS flight_delays (
  DelayId BIGINT AUTO_INCREMENT PRIMARY KEY,
  date VARCHAR(16),
  delay INT,
  distance INT,
  origin VARCHAR(8),
  destination VARCHAR(8),
  upload_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


