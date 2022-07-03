CLEAR CACHE
SELECT COUNT(*) FROM fireCalls
CACHE TABLE fireCalls
--8
SELECT `Unit Type`, COUNT(*) AS c
FROM fireCalls
GROUP BY `Unit Type`
ORDER BY c DESC
SET spark.sql.shuffle.partitions=2
SELECT `Unit Type`, COUNT(*) AS c
FROM fireCalls
GROUP BY `Unit Type`
ORDER BY c DESC
