DROP TABLE IF EXISTS canceled_flights_tmp;
CREATE TEMPORARY TABLE canceled_flights_tmp AS 
SELECT f.uniquecarrier AS uniquecarrier,
       f.origin AS citycode
FROM flights f
WHERE f.cancelled <> 0;

SELECT c.description AS carrier,
       count(cft.uniquecarrier) AS canceled_flights,
       concat_ws(", ", collect_set(a.city)) AS cities
FROM canceled_flights_tmp cft
JOIN carriers c ON cft.uniquecarrier = c.code
JOIN airports a ON cft.citycode = a.iata
GROUP BY c.description
HAVING canceled_flights > 1
ORDER BY canceled_flights DESC;