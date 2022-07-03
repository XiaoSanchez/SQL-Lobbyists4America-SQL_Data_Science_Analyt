USE DATABRICKS;
CREATE TABLE IF NOT EXISTS fireCallsClean
USING parquet
OPTIONS (
  path "mnt/davis/fire-calls/fire-calls-clean.parquett"
)
SELECT * FROM fireCallsClean LIMIT 10
SELECT COUNT(`Call_Type_Group`) AS Group_Count, `Call_Type_Group`
FROM fireCallsClean
GROUP BY `Call_Type_Group`
ORDER BY Group_Count DESC
CREATE TABLE IF NOT EXISTS fireCallsGroupCleaned AS (
  SELECT *
  FROM fireCallsClean
  WHERE `Call_Type_Group` IN ("Potentially Life-Threatening", "Non Life-threatening")
)
SELECT COUNT(*)
FROM fireCallsGroupCleaned
DROP VIEW IF EXISTS fireCallsDF;
CREATE OR REPLACE TEMPORARY VIEW fireCallsDF AS (
SELECT `Call_Type`,`Fire_Prevention_District`, `Neighborhooods_-_Analysis_Boundaries`,`Number_of_Alarms`,`Original_Priority`,
`Unit_Type`,`Battalion`, `Call_Type_Group`
FROM fireCallsGroupCleaned
)
CREATE OR REPLACE TEMPORARY VIEW testTable
USING csv
OPTIONS (
  header "true",
  path "/liu_si_zhe@outlook.com/Call_Type_Group_lr/modeltest.csv",
  inferSchema "true"
)
SELECT *
FROM testTable
LIMIT 10
USE DATABRICKS;
DROP TABLE IF EXISTS predictions;
CREATE TEMPORARY VIEW predictions AS (
  SELECT cast(predictUDF(Call_Type,Fire_Prevention_District, `Neighborhooods_-_Analysis_Boundaries`,Number_of_Alarms,Original_Priority,
  Unit_Type,Battalion) as int) as Call_Type_Group, *
  FROM testTable
)
SELECT * FROM predictions LIMIT 10
