SELECT c.description, temp_table.count
FROM
(
  SELECT f.uniquecarrier AS uniquecarrier, count(f.uniquecarrier) AS count
  FROM flights f
  GROUP BY f.uniquecarrier
  ORDER BY count DESC
  LIMIT 1
) temp_table
JOIN carriers c ON (temp_table.uniquecarrier=c.code);
