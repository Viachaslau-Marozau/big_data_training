CREATE TABLE IF NOT EXISTS airports
(
  iata STRING, 
  airport STRING, 
  city STRING, 
  state STRING, 
  country STRING, 
  lat DOUBLE, 
  long DOUBLE
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



CREATE TABLE IF NOT EXISTS carriers 
(
  code STRING, 
  description STRING
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

CREATE TABLE IF NOT EXISTS flights
(
  dayofmonth INT,
  dayofweek INT,
  deptime INT,
  crsdeptime INT,
  arrtime INT,
  crsarrtime INT,
  uniquecarrier STRING,
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
PARTITIONED BY
(
  year INT,
  month INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

