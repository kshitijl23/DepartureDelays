USE flightdb;

SELECT COUNT(*) AS duplicate_groups
FROM (
  SELECT DelayId
  FROM flightdb.flight_delays
  GROUP BY DelayId
  HAVING COUNT(*) > 1
) d;