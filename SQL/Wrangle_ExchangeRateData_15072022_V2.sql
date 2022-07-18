/* A project on cleaning data in SQL. 
The data to be cleansed is Exchange Rate Data extracted from CBN's official website on the 15th of July, 2022.
-- ----------------------------------------------------
1. The dataset was imported as a flat file
As a data validation policy, 
2. The Rate_Date was imported as a date field
3. NULLS were not allowed in any relevant column
4. The datatype specified for the rate fields were DECIMAL: Precision 18 and Scale 4 which is typical of exchange rates
*/
USE [DataCleaningProjects];
GO

-- Querying the metadata of the data source
SELECT *
FROM DataCleaningProjects.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'exchange15072022';

SELECT COUNT(*) FROM [dbo].[exchange15072022]; -- 49661 rows imported

-- Copying the relevant data to a new table for processing
CREATE TABLE [dbo].[ExchangeRateData]
(
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[RateDate] [date] NOT NULL,
	[Currency] [nvarchar](50) NOT NULL,
	[RateYear] [smallint] NOT NULL,
	[RateMonth] [nvarchar](50) NOT NULL,
	[BuyingRate] [decimal](18, 4) NOT NULL,
	[CentralRate] [decimal](18, 4) NOT NULL,
	[SellingRate] [decimal](18, 4) NOT NULL,
);
GO
INSERT INTO [dbo].[ExchangeRateData] (RateDate, Currency, RateYear, RateMonth, BuyingRate, CentralRate, SellingRate)
SELECT
	Rate_Date, Currency, Rate_Year, Rate_Month, Buying_Rate, Central_Rate, Selling_Rate
FROM [dbo].[exchange15072022];
GO

DROP TABLE [dbo].[exchange15072022];
GO

-- Eyeballing the inserted data
SELECT TOP 10 PERCENT * 
FROM [dbo].[ExchangeRateData]
ORDER BY 1 ASC;

-- Inspecting the dataset for NULL values
SELECT *
FROM [dbo].[ExchangeRateData]
WHERE RateDate IS NULL OR Currency IS NULL OR RateYear IS NULL OR RateMonth IS NULL
	OR BuyingRate IS NULL OR CentralRate IS NULL OR SellingRate IS NULL;
	-- No missing or NULL values found in the dataset
GO

-- ----------------------------------------------------------------------
-- Profiling the 'RateDate' Field
SELECT MIN(RateDate) AS MinimumDate FROM [dbo].[ExchangeRateData]; -- 2001-12-10
SELECT MAX(RateDate) AS MaximumDate FROM [dbo].[ExchangeRateData]; -- 2022-07-22

-- Retrieving records beyond the current date
SELECT *
FROM [dbo].[ExchangeRateData]
WHERE RateDate > CAST(GETDATE() AS DATE) -- There is on only one such record

SELECT DISTINCT
	DATENAME(WEEKDAY, RateDate) Wkdy, DATEPART(WEEKDAY, RateDate) WkNum, COUNT(Id) OVER (PARTITION BY datepart(WEEKDAY, RateDate)) AS Observations
        , FORMAT(COUNT(Id) OVER (PARTITION BY datepart(WEEKDAY, ratedate))/CAST(COUNT(Id) OVER () AS DECIMAL), 'P') AS Percent_Contribution
FROM [dbo].[ExchangeRateData] 
ORDER BY 3 DESC;
/* There were no records for Saturday, and only 9 records for Sunday which is grossly insufficient. 
We will delete the records for Sunday to avoid skewing our data, and work with the business day time-series data. 
*/

-- Normalizing the RateDate field
DELETE FROM ExchangeRateData
WHERE DATEPART(WEEKDAY, ratedate) = 1
	OR RateDate > CAST(GETDATE() AS DATE);

-- -----------------------------------------------------------------------
-- Profiling the 'Currency' field
SELECT DISTINCT Currency, LEN(Currency) as CharLength, COUNT(Id) over (partition by Currency) AS Observations
FROM ExchangeRateData
ORDER BY 1 ASC;
-- There are either trailing or leading spaces or misspelt currencies

UPDATE ExchangeRateData
SET Currency = TRIM(Currency);

UPDATE ExchangeRateData
SET Currency = 'DANISH KRONE'
WHERE Currency IN ('DANISH KRONA', 'DANISH KRONER');

UPDATE ExchangeRateData
SET Currency = 'POUND STERLING'
WHERE Currency LIKE '%POUND%';

UPDATE ExchangeRateData
SET Currency = 'EURO'
WHERE Currency LIKE '%EURO%';

UPDATE ExchangeRateData
SET Currency = 'SDR'
WHERE Currency LIKE '%SDR%';

UPDATE ExchangeRateData
SET Currency = 'SWISS FRANC'
WHERE Currency LIKE '%SWISS%';

-- Eyeballing the distribution of the Currency field
SELECT DISTINCT
	Currency, COUNT(Id) over (partition by Currency) as observations
		, FORMAT(COUNT(Id) OVER (PARTITION BY Currency)/CAST(COUNT(Id) over () as decimal), 'P') as ratio_to_report
FROM ExchangeRateData
ORDER BY 2 ASC;
/* The JAPANESE YEN, POESO, and NAIRA contribute 0.03% to the total number of observations, which can potentially skew the results of our analysis considering the entries for other currencies.
	We'll expunge the records for these currencies due to insufficient data */

DELETE FROM ExchangeRateData
WHERE Currency IN ('JAPANESE YEN', 'POESO', 'NAIRA');

-- ----------------------------------------------------------------
/* Inspecting the dataset for duplicate entries
Rule: There ought to be a singular currency observation for each day
*/
SELECT COUNT(*) FROM ExchangeRateData; -- 49639
SELECT COUNT(*) FROM (
	SELECT DISTINCT RateDate, Currency FROM ExchangeRateData ) tbl; -- 49600

-- Eyeballing the duplicated records
WITH BASE AS (
	SELECT RateDate, Currency, Id
		, ROW_NUMBER() OVER (PARTITION BY CONCAT(RateDate, Currency) ORDER BY RateDate ASC) row_num
	FROM ExchangeRateData
) 
SELECT
	RateDate, Currency, Id
FROM BASE
WHERE row_num > 1
ORDER BY 1, 2 ASC; -- There are 39 records to be expunged from the dataset

-- Deduplicating the dataset
WITH base AS (
	SELECT RateDate, Currency, Id
		, ROW_NUMBER() OVER (PARTITION BY CONCAT(RateDate, Currency) ORDER BY RateDate ASC) row_num
	FROM ExchangeRateData
) 
DELETE FROM ExchangeRateData
WHERE Id IN 
	(SELECT Id FROM base 
		WHERE row_num > 1); 

-- ------------------------------------------------------------------
/* Profiling the 'RateYear' and 'RateMonth' fields
Rule: The RateYear and RateMonth fields ought to be equal to the derived Year and Month features from the RateDate
*/
WITH base AS (
	SELECT
		Id, RateDate, Currency, RateYear, RateMonth, YEAR(RateDate) DerivedYear, DATENAME(MONTH, RateDate) DerivedMonth
			, CASE
				WHEN RateYear != YEAR(RateDate)
					OR RateMonth != DATENAME(MONTH, RateDate)
					THEN 1 ELSE 0 END AS Checker
	FROM ExchangeRateData
) 
SELECT
	Id, RateDate, Currency, RateYear, DerivedYear, RateMonth, DerivedMonth
INTO InconsistentRecords
FROM base 
WHERE Checker = 1
ORDER BY 2 DESC;
-- We observe that less that 202 records have either inconsistent RateYears or RateMonths or both. 

-- Normalizing the 'RateYear' and 'RateMonth' fields
UPDATE [dbo].[ExchangeRateData]
SET RateYear = YEAR(RateDate), 
RateMonth = DATENAME(MONTH, RateDate)
WHERE Id IN (SELECT Id FROM [dbo].[InconsistentRecords]);
GO 
DROP TABLE [dbo].[InconsistentRecords];

-- Eyeballing the distribution of the derived RateYear field
SELECT
	RateYear, observations, FORMAT(ratio_to_report, 'P') rr, FORMAT(avg(ratio_to_report) over (), 'P') as mean_rr
		, FORMAT(ABS(ratio_to_report - avg(ratio_to_report) over ()), 'P') as abs_deviation
FROM (
	SELECT DISTINCT
		RateYear, count(Id) over (partition by RateYear) as observations
			, COUNT(Id) OVER (PARTITION BY RateYear)/CAST(COUNT(Id) over () as decimal) as ratio_to_report
	FROM ExchangeRateData ) tbl
ORDER BY 2 ASC;
-- Year 2001 contibuted less than 1% to the total number of observations which is relatively small, and require further investigation

SELECT
	FORMAT(RateDate, 'yyyy-MM') as yrmth, count(Id) as observations
FROM ExchangeRateData
WHERE RateYear = 2001
GROUP BY FORMAT(RateDate, 'yyyy-MM')
ORDER BY 1 ASC;

/* Further, there were observations recorded ONLY in December 2001. Since we have no reason why there were so little observations, 
we can delete these records due to insufficient data and work with the other years to avoid skewing the results of our analysis. */

DELETE FROM ExchangeRateData
WHERE RateYear = 2001;

-- Eyeballing the distribution of the derived RateMonth field
SELECT
	RateMonth, observations, FORMAT(ratio_to_report, 'P') rr, FORMAT(avg(ratio_to_report) over (), 'P') as mean_rr
		, FORMAT(ABS(ratio_to_report - avg(ratio_to_report) over ()), 'P') as abs_deviation
FROM (
	SELECT DISTINCT
		RateMonth, count(Id) over (partition by RateMonth) as observations
			, COUNT(Id) OVER (PARTITION BY RateMonth)/CAST(COUNT(Id) over () as decimal) as ratio_to_report
	FROM ExchangeRateData ) tbl
ORDER BY 2 ASC;

-- Inspecting the latitude of the time series data for the USD Exchange Rates
WITH dt AS (
SELECT DISTINCT
	RateYear, MONTH(RateDate) AS RateMonthIndex, DATEPART(WEEK, RateDate) AS RateWeekIndex, COUNT(Id) OVER (PARTITION BY RateYear) DataPoints
FROM [dbo].[ExchangeRateData]
)
SELECT
	RateYear, DataPoints, COUNT(DISTINCT RateMonthIndex) MaxObservedMth, COUNT(DISTINCT RateWeekIndex) MaxObservedWk
FROM dt
GROUP BY RateYear, DataPoints
ORDER BY 1 ASC;

/* We observe that the scope of observation for the time series data is complete at 12 months or 52/53 weeks each year from 2002 thru' 2021, 
which is suitable for further processing and analysis. */

-- ------------------------------------------------------------------------
-- Creating a view for the US DOLLAR time series data
GO
CREATE VIEW USDExchangeRateData AS 
SELECT 
	ROW_NUMBER() OVER (ORDER BY RATEDATE DESC) AS Id, 
		RateDate, Currency, RateYear, RateMonth, BuyingRate, CentralRate, SellingRate
FROM [dbo].[ExchangeRateData]
WHERE Currency = 'US DOLLAR';
GO

SELECT * FROM [dbo].[USDExchangeRateData]
ORDER BY 1 ASC;

-- ---------------------------------------------------------------------------
-- Descriptive statistics to understand the distribution of values in the rates fields
DECLARE @rate VARCHAR(50)
DECLARE @sql NVARCHAR(MAX)
SET @rate = 'BuyingRate' -- Change parameter value to other rate categories and execute
SET @sql  = 
'select distinct
	RateYear, count(Id) over (partition by RateYear) as num_of_observations, min('+ @rate + ') over (partition by RateYear) as min_rate
	, max('+ @rate + ') over (partition by RateYear) as max_rate, AVG('+ @rate + ') over (partition by RateYear) as avg_rate
		, percentile_cont(0.25) within group (order by '+ @rate + ' asc) over(partition by RateYear) as "25th_percentile"
			, percentile_cont(0.5) within group (order by '+ @rate + ' asc) over(partition by RateYear) as median_value
				, percentile_cont(0.75) within group (order by '+ @rate + ' asc) over(partition by RateYear) as "75th_percentile"
	, stdev('+ @rate + ') over (partition by RateYear) as standard_deviation
from [dbo].[USDExchangeRateData]
ORDER BY 1 ASC'
PRINT @sql
EXECUTE sp_executesql @sql
/*
-- The median rates have consistently been on an increase year over year since 2014; increasing by 167% to date.
-- The max central rate of 1376.8000NGN/USD in 2003 is extremely far from the typical/average rate that year
-- The min central rates of 74.5200NGN/USD and 66.0900NGN/USD in 2008 and 2009 respectively are abnormal and inconsistent with the median/average rates in those years
-- The min buying rates of 24.0200NGN/USD and 15.5900NGN/USD in 2007 and 2008 respectively are abnormal and inconsistent with the median/average rates in those years
-- Years 2016, 2020 and 2021 had double digit standard deviations across the rate categories. 
In addition, the central rate of 2003 had a double digit standard deviation most likely due to the abnormal max central rate of 1376.8000NGN/USD which skewed the distribution and mean.
*/

-- Examining the absolute deviation from the lateral average of typical rates
GO
WITH mid as (
select distinct
	RateYear, count(Id) over (partition by RateYear) as num_of_observations
	, percentile_cont(0.5) within group (order by BuyingRate asc) over(partition by RateYear) as mid_buy
		, percentile_cont(0.5) within group (order by CentralRate asc) over(partition by RateYear) as mid_central
		, percentile_cont(0.5) within group (order by SellingRate asc) over(partition by RateYear) as mid_sell
from [dbo].[USDExchangeRateData]
) 
SELECT
	RateYear, num_of_observations, mid_buy, mid_central, mid_sell
		, FLOOR(ABS(mid_buy-ty_avg))+FLOOR(ABS(mid_central-ty_avg))+FLOOR(ABS(mid_sell-ty_avg)) MinimumDistance
FROM (
	SELECT
		RateYear, num_of_observations, mid_buy, mid_central, mid_sell, (mid_buy+mid_central+mid_sell)/3 ty_avg
	FROM mid ) tbl
ORDER BY 1 ASC;
/* The minimum distance is consistently zero, that is, there is "typically" no difference between the rates, and there ought to be no deviation 
from the average rate at any given point in time. */

/*
-- Extracting the outliers
One statistical method of identifying outliers is through the use of the interquartile range, or IQR. When we find values that fall outside of 1.5 times the range 
between our first and third quartiles, we typically consider these to be outliers.
*/
-- Creating an empty temp table for the outliers
SELECT 1 as Id, RateDate, BuyingRate, CentralRate, SellingRate -- to avoid identity constraint on insert
INTO [dbo].[ExchangeRateOutliers]
FROM [dbo].[ExchangeRateData]
WHERE Id IS NULL;
GO

DECLARE @OutlierRate VARCHAR(50)
DECLARE @OutlierSQL NVARCHAR(MAX)
SET @OutlierRate = 'CentralRate' -- Change parameter value to other rate categories and execute
SET @OutlierSQL = 
'INSERT INTO [dbo].[ExchangeRateOutliers] (Id, RateDate, BuyingRate, CentralRate, SellingRate)
SELECT
	Id, RateDate, BuyingRate, CentralRate, SellingRate
FROM (
	SELECT 
		Id, RateDate, BuyingRate, CentralRate, SellingRate, (BuyingRate + CentralRate + SellingRate)/3 avg_rate
			, (t3.q_one - outlier_range) lowerbound, (t3.q_three + outlier_range) upperbound
	FROM [dbo].[USDExchangeRateData] t1
		JOIN (
				select	
					RateYear, q_one, q_three, 1.5*(q_three - q_one) as outlier_range
				from (
					select distinct
						RateYear, percentile_cont(0.25) within group (order by '+ @OutlierRate + ' asc) over(partition by RateYear) as q_one
							, percentile_cont(0.75) within group (order by '+ @OutlierRate + ' asc) over(partition by RateYear) as q_three
					from [dbo].[USDExchangeRateData] ) t2 
			) t3 ON t1.RateYear = t3.RateYear AND (t1.'+ @OutlierRate +' < (t3.q_one - outlier_range) OR t1.'+ @OutlierRate +' > (t3.q_three + outlier_range)) ) t4
WHERE (FLOOR(ABS(BuyingRate-avg_rate)) + FLOOR(ABS(CentralRate-avg_rate)) + FLOOR(ABS(SellingRate-avg_rate))) > 0 
	AND NOT EXISTS (SELECT 1 FROM [dbo].[ExchangeRateOutliers] t5 WHERE t4.Id = t5.Id)'
PRINT @OutlierSQL
EXECUTE sp_executesql @OutlierSQL
GO

SELECT * FROM [dbo].[ExchangeRateOutliers];
GO

-- ----------------------------------------------------
-- To resolve these outliers, we'll update the anomalous data points with the longitudinal average of the immediate preceding and following rates
DECLARE @Currency VARCHAR(50) = 'US DOLLAR'
UPDATE [dbo].[ExchangeRateData]
SET BuyingRate = e2.avg_BuyingRate, CentralRate = e2.avg_CentralRate, SellingRate = e2.avg_SellingRate
FROM [dbo].[ExchangeRateData] e1
JOIN (
	SELECT
		Id, RateDate, (pre_BuyingRate + fol_BuyingRate)/2 as avg_BuyingRate, (pre_CentralRate + fol_CentralRate)/2 as avg_CentralRate
			, (pre_SellingRate + fol_SellingRate)/2 as avg_SellingRate
	FROM (
		SELECT 
			Id, RateDate
			, LAG(BuyingRate, 1, BuyingRate) OVER (ORDER BY RateDate ASC) AS pre_BuyingRate
			, LEAD(BuyingRate, 1, BuyingRate) OVER (ORDER BY RateDate ASC) AS fol_BuyingRate
			, LAG(CentralRate, 1, CentralRate) OVER (ORDER BY RateDate ASC) AS pre_CentralRate
			, LEAD(CentralRate, 1, CentralRate) OVER (ORDER BY RateDate ASC) AS fol_CentralRate
			, LAG(SellingRate, 1, CentralRate) OVER (ORDER BY RateDate ASC) AS pre_SellingRate
			, LEAD(SellingRate, 1, CentralRate) OVER (ORDER BY RateDate ASC) AS fol_SellingRate
		FROM [dbo].[USDExchangeRateData] ) t1
	WHERE EXISTS (SELECT 1 
					FROM [dbo].[ExchangeRateOutliers] t2
					WHERE t1.Id = t2.Id ) 
) e2 ON CONCAT(e1.RateDate, e1.Currency) = CONCAT(e2.RateDate, @Currency);
GO
DROP TABLE [dbo].[ExchangeRateOutliers]
GO

-- ---------------------------------------------------------
-- The data is a business day time series data, and to avoid the interpretation of a non-existent daily trend, we'll transform the rates to monthly averages
SELECT DISTINCT
	Currency, RateYear, MONTH(RateDate) as RateMonth, AVG(BuyingRate) OVER (PARTITION BY RateYear, RateMonth) AS AvgMthlyBuyingRate
		, AVG(CentralRate) OVER (PARTITION BY RateYear, RateMonth) AS AvgMthlyCentralRate
		, AVG(SellingRate) OVER (PARTITION BY RateYear, RateMonth) AS AvgMthlySellingRate
FROM [dbo].[USDExchangeRateData]
ORDER BY 2, 3 ASC;

-- How has the NGN/USD exchange rate changed since 2002?
WITH base as (
	SELECT DISTINCT
		Currency, RateYear, MONTH(RateDate) as RateMonth, AVG(BuyingRate) OVER (PARTITION BY RateYear, RateMonth) AS AvgMthlyBuyingRate
			, AVG(CentralRate) OVER (PARTITION BY RateYear, RateMonth) AS AvgMthlyCentralRate
			, AVG(SellingRate) OVER (PARTITION BY RateYear, RateMonth) AS AvgMthlySellingRate
	FROM [dbo].[USDExchangeRateData]
)
SELECT
	Currency, RateYear, AvgMthlyCentralRate, FORMAT((AvgMthlyCentralRate - PrevAvgMthlyCentralRate)/PrevAvgMthlyCentralRate, 'P') AS ChangeRate
FROM (
	SELECT
		Currency, RateYear, AvgMthlyCentralRate, LAG(AvgMthlyCentralRate) OVER (ORDER BY RateYear ASC) AS PrevAvgMthlyCentralRate
	FROM base
	WHERE (RateMonth = 12 AND RateYear < YEAR(GETDATE())) OR (RateYear = YEAR(GETDATE()) AND RateMonth = MONTH(GETDATE()))
	) tbl
ORDER BY 2 ASC;

-- How has the USD/NGN exchange rate changed since 2002?
WITH base as (
	SELECT DISTINCT
		Currency, RateYear, MONTH(RateDate) as RateMonth, AVG(BuyingRate) OVER (PARTITION BY RateYear, RateMonth) AS AvgMthlyBuyingRate
			, AVG(CentralRate) OVER (PARTITION BY RateYear, RateMonth) AS AvgMthlyCentralRate
			, AVG(SellingRate) OVER (PARTITION BY RateYear, RateMonth) AS AvgMthlySellingRate
	FROM [dbo].[USDExchangeRateData]
)
SELECT
	Currency, RateYear, USDNGN, FORMAT((USDNGN - PrevUSDNGN)/PrevUSDNGN, 'P') AS ChangeRate
FROM (
	SELECT
		Currency, RateYear, 1/AvgMthlyCentralRate as USDNGN, LAG(1/AvgMthlyCentralRate) OVER (ORDER BY RateYear ASC) AS PrevUSDNGN
	FROM base
	WHERE (RateMonth = 12 AND RateYear < YEAR(GETDATE())) OR (RateYear = YEAR(GETDATE()) AND RateMonth = MONTH(GETDATE()))
	) tbl
ORDER BY 2 ASC;


