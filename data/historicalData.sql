/****** Script for SelectTopNRows command from SSMS  ******/


-- WT
-- gb: 2498
-- tb: 3056, 3059, 3064, 3072
-- bop: 2371
SELECT TOP (10000000) * 
FROM CAW_OOS.[dbo].[WaterTemperature] 
WHERE observationDate BETWEEN '2023-04-06 03:46:56.266' AND '2024-07-06 03:46:56.266'
  AND sensorId IN (3056, 3059, 3064, 3072)
  AND observationDate > '1900-01-01 00:00:00.000'
  AND (qualityFlag IS NULL OR qualityFlag IN (0, 3))
  AND COALESCE(sensorId, SensorId) IS NOT NULL;

SELECT TOP (10000000) * 
FROM CAW_OOS.[dbo].[WaterTemperature] 
WHERE observationDate BETWEEN '2023-04-06 03:46:56.266' AND '2024-07-06 03:46:56.266'
  AND sensorId IN (2498)
  AND observationDate > '1900-01-01 00:00:00.000'
  AND (qualityFlag IS NULL OR qualityFlag IN (0, 3))
  AND COALESCE(sensorId, SensorId) IS NOT NULL;

SELECT TOP (10000000) * 
FROM CAW_OOS.[dbo].[WaterTemperature] 
WHERE observationDate BETWEEN '2023-04-06 03:46:56.266' AND '2024-07-06 03:46:56.266'
  AND sensorId IN (2371)
  AND observationDate > '1900-01-01 00:00:00.000'
  AND (qualityFlag IS NULL OR qualityFlag IN (0, 3))
  AND COALESCE(sensorId, SensorId) IS NOT NULL;


-- Wind
-- gb: 2504
-- tb: 3070
-- bop: 2377
SELECT TOP (10000000) * 
FROM CAW_OOS.[dbo].[Winds] 
WHERE observationDate BETWEEN '2023-04-06 03:46:56.266' AND '2024-07-06 03:46:56.266'
  AND sensorId IN (3070)
  AND observationDate > '1900-01-01 00:00:00.000'
  AND (qualityFlag IS NULL OR qualityFlag IN (0, 3))
  AND COALESCE(sensorId, SensorId) IS NOT NULL;

SELECT TOP (10000000) * 
FROM CAW_OOS.[dbo].[Winds] 
WHERE observationDate BETWEEN '2023-04-06 03:46:56.266' AND '2024-07-06 03:46:56.266'
  AND sensorId IN (2504)
  AND observationDate > '1900-01-01 00:00:00.000'
  AND (qualityFlag IS NULL OR qualityFlag IN (0, 3))
  AND COALESCE(sensorId, SensorId) IS NOT NULL;


SELECT TOP (10000000) * 
FROM CAW_OOS.[dbo].[Winds] 
WHERE observationDate BETWEEN '2023-04-06 03:46:56.266' AND '2024-07-06 03:46:56.266'
  AND sensorId IN (2377)
  AND observationDate > '1900-01-01 00:00:00.000'
  AND (qualityFlag IS NULL OR qualityFlag IN (0, 3))
  AND COALESCE(sensorId, SensorId) IS NOT NULL;


-- Waves
-- gb: 2493
-- tb: 3087
-- bop: 2366
SELECT TOP (10000000) * 
FROM CAW_OOS.[dbo].[waveSummary] 
WHERE observationDate BETWEEN '2023-04-06 03:46:56.266' AND '2024-07-06 03:46:56.266'
  AND sensorId IN (3087)
  AND observationDate > '1900-01-01 00:00:00.000'
  AND (qualityFlag IS NULL OR qualityFlag IN (0, 3))
  AND COALESCE(sensorId, SensorId) IS NOT NULL;

SELECT TOP (10000000) * 
FROM CAW_OOS.[dbo].[waveSummary] 
WHERE observationDate BETWEEN '2023-04-06 03:46:56.266' AND '2024-07-06 03:46:56.266'
  AND sensorId IN (2493)
  AND observationDate > '1900-01-01 00:00:00.000'
  AND (qualityFlag IS NULL OR qualityFlag IN (0, 3))
  AND COALESCE(sensorId, SensorId) IS NOT NULL;

SELECT TOP (10000000) * 
FROM CAW_OOS.[dbo].[waveSummary] 
WHERE observationDate BETWEEN '2023-04-06 03:46:56.266' AND '2024-07-06 03:46:56.266'
  AND sensorId IN (2366)
  AND observationDate > '1900-01-01 00:00:00.000'
  AND (qualityFlag IS NULL OR qualityFlag IN (0, 3))
  AND COALESCE(sensorId, SensorId) IS NOT NULL;