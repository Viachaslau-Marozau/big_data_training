SELECT flights.uniquecarrier AS uniquecarrier, count(flights.uniquecarrier) AS count
FROM flights
GROUP BY flights.uniquecarrier;
