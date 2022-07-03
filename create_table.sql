CREATE TABLE IF NOT EXISTS fireIncidents
USING csv
OPTIONS (
  header "true",
  path "/mnt/davis/fire-incidents/fire-incidents-2016.csv",
  inferSchema "true"
)
SELECT *
From fireIncidents
LIMIT 10
SELECT *
FROM fireIncidents
LIMIT 10
SELECT `Ignition Cause`,COUNT(*)
FROM fireIncidents
GROUP BY `Ignition Cause`
CREATE TABLE IF NOT EXISTS fireCalls
USING csv
OPTIONS (
  header "true",
  path "/mnt/davis/fire-calls/fire-calls-truncated-comma.csv",
  inferSchema "true"
)
SELECT COUNT(*)
FROM fireIncidents fi
JOIN fireCalls fc
ON fi.Battalion=fc.Battalion
