CREATE EXTERNAL TABLE IF NOT EXISTS cities_external
(
id INT,
name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES
(
'input.regex'='^([0-9]+).([a-z]+)$'
)
STORED AS TEXTFILE
LOCATION '/input_data/viachaslau_marozau/cities/';

CREATE EXTERNAL TABLE IF NOT EXISTS statistics_external
(
bid_id STRING,
tstamp STRING,
log_type STRING,
ip_in_id STRING,
user_agent STRING,
ip STRING,
region_id STRING,
city_id INT,
ad_exchange STRING,
domain STRING,
url STRING,
an_url STRING,
ad_slot_id STRING,
ad_slot_width STRING,
ad_slot_height STRING,
ad_slot_visibility STRING,
ad_slot_format STRING,
ad_slot_floor_price STRING,
creative_id STRING,
bidding_price STRING,
paying_price STRING,
landing_page_url STRING,
advertiser_id STRING,
user_profile_ids STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES
(
'input.regex'='^([0-9a-f]+)\\t(\\d+)\\t(\\d+)\\t([^\\t]+)\\t([^\\t]*)\\t([^\\t]+)\\t(\\d+)\\t(\\d+)\\t([^\\t]+)\\t([^\\t]+)\\t([^\\t]+)\\t([^\\t]+)\\t([^\\t]+)\\t(\\d+)\\t(\\d+)\\t([^\\t]+)\\t([^\\t]+)\\t(\\d+)\\t(\\d+)\\t(\\d+)\\t(\\d+)\\t([^\\t]+)\\t(\\d+)\\t([^\\t]+).*'
)
STORED AS TEXTFILE
LOCATION '/input_data/viachaslau_marozau/data/';

INSERT OVERWRITE TABLE cities
SELECT id,
       name
FROM cities_external;
DROP TABLE IF EXISTS  tmp_cities;

INSERT OVERWRITE TABLE statistic
SELECT se.city_id,
       se.user_agent
FROM statistics_external se;
DROP TABLE IF EXISTS statistics_external;
