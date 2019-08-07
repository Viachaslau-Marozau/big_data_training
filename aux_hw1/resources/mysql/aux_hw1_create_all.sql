CREATE DATABASE IF NOT EXISTS weatherdb;

USE weatherdb;

CREATE TABLE IF NOT EXISTS weather
(
  stateId VARCHAR(100),
  date DATE ,
  tmin INT,
  tmax INT,
  snow INT,
  snwd INT,
  prcp INT
 );

CREATE TABLE IF NOT EXISTS weather_tmp
(
  stateId VARCHAR(100),
  date DATE ,
  tmin INT,
  tmax INT,
  snow INT,
  snwd INT,
  prcp INT
);