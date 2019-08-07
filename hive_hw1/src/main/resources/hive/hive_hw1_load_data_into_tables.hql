LOAD DATA INPATH '/input_data/viachaslau_marozau/airports/*.csv'
OVERWRITE INTO TABLE airports;

LOAD DATA INPATH '/input_data/viachaslau_marozau/carriers/*.csv'
OVERWRITE INTO TABLE carriers;

CREATE TEMPORARY TABLE load_flights 
(
  year INT,
  month INT,
  dayofmonth INT,
  dayofweek INT,
  deptime INT,
  crsdeptime INT,
  arrtime INT,
  crsarrtime INT,
  uniquecarrier string,
  flightnum INT,
  tailnum STRING,
  actualelapsedtime INT,
  crselapsedtime INT,
  airtime INT,
  arrdelay INT,
  depdelay INT,
  origin STRING,
  dest STRING,
  distance INT,
  taxiin INT,
  taxiout INT,
  cancelled INT,
  cancellationcode STRING,
  diverted INT,
  carrierdelay INT,
  weatherdelay INT,
  nasdelay INT,
  securitydelay INT,
  lateaircraftdelay INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/input_data/viachaslau_marozau/flights/*.csv'
OVERWRITE INTO TABLE load_flights;

SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.support.sql11.reserved.keywords=false;

INSERT OVERWRITE TABLE flights PARTITION (year, month) 
SELECT
  dayofmonth,
  dayofweek,
  deptime,
  crsdeptime,
  arrtime,
  crsarrtime,
  uniquecarrier,
  flightnum,
  tailnum,
  actualelapsedtime,
  crselapsedtime,
  airtime,
  arrdelay,
  depdelay,
  origin,
  dest,
  distance,
  taxiin,
  taxiout,
  cancelled,
  cancellationcode,
  diverted,
  carrierdelay,
  weatherdelay,
  nasdelay,
  securitydelay,
  lateaircraftdelay,
  year,
  month
FROM load_flights;
