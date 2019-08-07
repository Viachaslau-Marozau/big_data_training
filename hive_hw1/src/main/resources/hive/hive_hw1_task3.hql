SELECT temp_table.airport, sum(temp_table.count) as count FROM
(
SELECT a1.airport, count(a1.airport) AS count FROM airports a1
  INNER JOIN flights f1 ON a1.iata=f1.dest
  WHERE f1.month BETWEEN 6 AND 8 AND a1.country='USA'
  GROUP BY a1.airport
  UNION ALL
  SELECT a2.airport, count(a2.airport) AS count FROM airports a2
  INNER JOIN flights f2 ON a2.iata=f2.origin
  WHERE f2.month BETWEEN 6 AND 8 AND a2.country='USA'
  GROUP BY a2.airport
) temp_table
GROUP BY temp_table.airport
SORT BY count DESC
LIMIT 5;
