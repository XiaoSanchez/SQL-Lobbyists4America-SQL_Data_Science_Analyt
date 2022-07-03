CREATE EXTERNAL TABLE IF NOT EXISTS newTable (
  `Address` STRING,
  `City` STRING,
  `Battalion` STRING,
  `Box` STRING
)
LOCATION '/tmp/newTableLoc'
DESCRIBE EXTENDED newTable
CREATE OR REPLACE TEMPORARY VIEW  fireCallsJSON (
  `Call Number` INT,
  `Unit ID` STRING,
  `Incident Number` INT,
  `Call Type` STRING,
  `Call Date` STRING,
  `Watch Date` STRING,
  `Received DtTm` STRING,
  `Entry DtTm` STRING,
  `Dispatch DtTm` STRING,
  `Response DtTm` STRING,
  `On Scene DtTm` STRING,
  `Transport DtTm` STRING,
  `Hospital DtTm` STRING,
  `Call Final Disposition` STRING,
  `Available DtTm` STRING,
  `Address` STRING,
  `City` STRING,
  `Zipcode of Incident` INT,
  `Battalion` STRING,
  `Station Area` STRING,
  `Box` STRING,
  `Original Priority` STRING,
  `Priority` INT,
  `Final Priority` INT,
  `ALS Unit` BOOLEAN,
  `Call Type Group` STRING,
  `Number of Alarms` INT,
  `Unit Type` STRING,
  `Unit sequence in call dispatch` INT,
  `Fire Prevention District` STRING,
  `Supervisor District` STRING,
  `Neighborhooods - Analysis Boundaries` STRING,
  `Location` STRING,
  `RowID` STRING
)
USING JSON 
OPTIONS (
    path "/mnt/davis/fire-calls/fire-calls-truncated.json"
);
DESCRIBE fireCallsJSON
INSERT INTO newTable
SELECT Address, City, Battalion, Box
FROM fireCallsJSON
WHERE `Final Priority`=3; 
SELECT * FROM newTable;
SELECT COUNT(*) FROM newTable
SELECT * FROM newTable
ORDER BY Battalion ASC
CREATE TABLE IF NOT EXISTS newTablePartitioned
AS
SELECT /*+ REPARTITION(256) */ *
FROM newTable
DESCRIBE EXTENDED newTablePartitioned
SELECT * FROM newTablePartitioned ORDER BY `Battalion`
--DROP TABLE newTable;
 SELECT * FROM newTable;
