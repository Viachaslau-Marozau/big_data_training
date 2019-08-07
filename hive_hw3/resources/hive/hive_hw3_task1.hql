ADD JAR hdfs:///input_data/viachaslau_marozau/hive_hw3-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION ua_parser as 'com.epam.courses.hive.udf.UserAgentParserUDF';

CREATE TEMPORARY TABLE full_statistics AS
SELECT
  outer_temp_table.city_id,
  outer_temp_table.ua_key,
  outer_temp_table.ua_value
FROM (
	SELECT inner_temp_table.city_id AS city_id,
  		   inner_temp_table.ua_key AS ua_key,
  		   inner_temp_table.ua_value AS ua_value,
  		   rank() OVER
  				(PARTITION BY inner_temp_table.city_id,
				 			  inner_temp_table.ua_key
				 ORDER BY inner_temp_table.ua_count DESC) AS ua_rank
	FROM
		(
			SELECT st.city_id AS city_id,
		  		   key AS ua_key,
		           value AS ua_value,
		           count(*) as ua_count
			FROM statistic st
    		LATERAL VIEW explode(ua_parser(st.user_agent)) dummy AS key, value
			GROUP BY st.city_id, key, value
		) inner_temp_table
	) outer_temp_table
WHERE outer_temp_table.ua_rank = 1;

SELECT c.name AS city,
       fsd.ua_value AS device,
       fso.ua_value AS os,
       fsb.ua_value AS browser
FROM cities c
JOIN full_statistics fsd ON c.id = fsd.city_id AND fsd.ua_key = 'device'
JOIN full_statistics fso ON c.id = fso.city_id AND fso.ua_key = 'os'
JOIN full_statistics fsb ON c.id = fsb.city_id AND fsb.ua_key = 'browser'
ORDER BY city ASC;