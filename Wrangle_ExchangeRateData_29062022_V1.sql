/* 	A data cleaning project. 
	The data to be cleansed is Exchange Rate Data scraped from CBN's official website https://www.cbn.gov.ng/rates/ExchRateByCurrency.asp 
    and was last updated on 6/23/2022. The script Insert_ExchangeRateData_23062022.sql must be run to import data. 
*/
--DROP TABLE [dbo].[ExchangeRateData]
GO
USE [richprojects]
GO
-- Eyeballing the inserted data
SELECT COUNT(*) FROM ExchangeRateData; -- 49505 rows imported

SELECT * FROM [dbo].[ExchangeRateData] 
WHERE [Id] IN (
	SELECT TOP 1 PERCENT [Id] 
	FROM [dbo].[ExchangeRateData] 
	ORDER BY NEWID()
	);

-- ----------------------------------------------------------------------
-- Normalizing the 'RateDate' Field
SELECT DISTINCT
	DATEPART(WEEKDAY, ratedate) WkNum, DATENAME(weekday, ratedate) Wkdy, COUNT(Id) OVER (PARTITION BY datepart(WEEKDAY, ratedate)) AS Observations
        , FORMAT(COUNT(Id) OVER (PARTITION BY datepart(WEEKDAY, ratedate))/CAST(COUNT(Id) over () as decimal), 'P') as ratio_to_report
FROM [dbo].[ExchangeRateData] 
ORDER BY 3 DESC;
/* We observe that there were no observations on Saturday, and only 9 records for Sunday. To avoid skewing the results of our analysis due to insufficient data, 
 we can delete these records-- and work with the business day time-series data*/

DELETE FROM ExchangeRateData
WHERE DATEPART(WEEKDAY, ratedate) = 1;

-- -----------------------------------------------------------------------
-- Normalizing the 'Currency' field
SELECT DISTINCT Currency, LEN(Currency) as char_len, COUNT(Id) over (partition by Currency) AS observations
FROM ExchangeRateData
ORDER BY 1 ASC;
-- We observe duplications as a result of either trailing or leading spaces or misspelt currencies

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

-- Eyeballing the distribution of the cleaned Currency field
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
-- Inspecting the dataset for duplicate entries
SELECT COUNT(*) FROM ExchangeRateData; -- 49484
-- based on the fact that there ought to be a singular currency observation for each day
SELECT COUNT(*) FROM (
	SELECT DISTINCT RateDate, Currency FROM ExchangeRateData ) tbl; -- 49445

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
ORDER BY 1, 2 ASC;
-- There are 39 records to be expunged from the dataset

-- Expunging the duplicated records
WITH base AS (
	SELECT RateDate, Currency, Id
		, ROW_NUMBER() OVER (PARTITION BY CONCAT(RateDate, Currency) ORDER BY RateDate ASC) row_num
	FROM ExchangeRateData
) 
DELETE FROM ExchangeRateData
WHERE Id IN 
	(SELECT Id FROM base 
		WHERE row_num > 1); -- 39 rows affected

-- ------------------------------------------------------------------
-- Normalizing the 'RateYear' field
SELECT DISTINCT RateYear, LEN(RateYear) as char_len
	, COUNT(Id) over (partition by RateYear) AS observations, MAX(YEAR(RateDate)) over () MaxYr
FROM ExchangeRateData
ORDER BY 1 DESC;
-- We can observe that there is a futuristic RateYear- 2023 in contrast to our max derived year from RateDate of 2022

-- Deriving the year feature from RateDate and updating the RateYear field
UPDATE [dbo].[ExchangeRateData]
SET RateYear = YEAR(RateDate);

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
-- The contribution of 2001 to the total observations is abnormally small, and require further investigation

SELECT
	FORMAT(RateDate, 'yyyy-MM') as yrmth, count(Id) as observations
FROM ExchangeRateData
WHERE RateYear = 2001
GROUP BY FORMAT(RateDate, 'yyyy-MM')
ORDER BY 1 ASC;

/* Further, there were observations recorded ONLY in December 2001. Since we have no reason why there were so little observations, 
we can delete these records and work with the other years to avoid skewing the results of our analysis. */

DELETE FROM ExchangeRateData
WHERE RateYear = 2001;

-- --------------------------------------------------------------------------------
-- Normalizing the 'RateMonth' field
SELECT DISTINCT RateMonth, LEN(RateMonth) as char_len, COUNT(Id) over (partition by RateMonth) AS observations
FROM ExchangeRateData
ORDER BY 1 ASC;
-- We ought to have only 12 months, but there are observable anomalies

-- Deriving the month feature from RateDate and updating the RateMonth field
UPDATE [dbo].[ExchangeRateData]
SET RateMonth = DATENAME(MONTH, RateDate);

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

-- ------------------------------------------------------------------------
-- Creating a view for the US DOLLAR time series data based on the subject matter requirement
GO
CREATE VIEW USDExchangeRateData 
AS 
SELECT 
	ROW_NUMBER() OVER (ORDER BY RATEDATE DESC) AS Id, 
		RateDate, Currency, RateYear, RateMonth, BuyingRate, CentralRate, SellingRate
FROM [dbo].[ExchangeRateData]
WHERE Currency LIKE 'US%';

SELECT TOP 10 PERCENT * FROM [dbo].[USDExchangeRateData];

-- Inspecting the latitude of the time series data for the USD Exchange Rates
WITH dt AS (
SELECT DISTINCT
	RateYear, MONTH(RateDate) AS RateMonthIndex, DATEPART(WEEK, RateDate) AS RateWeekIndex, COUNT(Id) OVER (PARTITION BY RateYear) DataPoints
FROM [dbo].[USDExchangeRateData]
)
SELECT
	RateYear, MAX(RateMonthIndex) MaxObservedMth, MAX(RateWeekIndex) MaxObservedWk, DataPoints
FROM dt
GROUP BY RateYear, DataPoints
ORDER BY 1 ASC;
/* We observe that the scope of observation for the time series data is complete at 12 months or 52/53 weeks each year from 2002 thru' 2021, 
and there is an approximately equal number of data points year over year, which is suitable for further processing and analysis. */

-- ------------------------------------------------------------------------
-- Inspecting the quality of the rates data
-- Examining the rates fields for missing prices
SELECT *
FROM [dbo].[USDExchangeRateData]
WHERE BuyingRate IS NULL OR CentralRate IS NULL OR SellingRate IS NULL;
GO
-- There are no missing values

-- Descriptive statistics to understand the distribution of values in the rates fields
select distinct
	RateYear, 'BuyingRate' as Rate, count(Id) over (partition by RateYear) as num_of_observations, min(BuyingRate) over (partition by RateYear) as min_rate
	, max(BuyingRate) over (partition by RateYear) as max_rate, AVG(BuyingRate) over (partition by RateYear) as avg_rate
		, percentile_cont(0.25) within group (order by BuyingRate asc) over(partition by RateYear) as "25th_percentile"
			, percentile_cont(0.5) within group (order by BuyingRate asc) over(partition by RateYear) as median_value
				, percentile_cont(0.75) within group (order by BuyingRate asc) over(partition by RateYear) as "75th_percentile"
	, stdev(BuyingRate) over (partition by RateYear) as standard_deviation
from [dbo].[USDExchangeRateData]
ORDER BY 1 ASC
GO
select distinct
	RateYear, 'CentralRate' as Rate, count(Id) over (partition by RateYear) as num_of_observations, min(CentralRate) over (partition by RateYear) as min_rate
	, max(CentralRate) over (partition by RateYear) as max_rate, AVG(CentralRate) over (partition by RateYear) as avg_rate
		, percentile_cont(0.25) within group (order by CentralRate asc) over(partition by RateYear) as "25th_percentile"
			, percentile_cont(0.5) within group (order by CentralRate asc) over(partition by RateYear) as median_value
				, percentile_cont(0.75) within group (order by CentralRate asc) over(partition by RateYear) as "75th_percentile"
	, stdev(CentralRate) over (partition by RateYear) as standard_deviation
from [dbo].[USDExchangeRateData]
ORDER BY 1 ASC
GO
select distinct
	RateYear, 'SellingRate' as Rate, count(Id) over (partition by RateYear) as num_of_observations, min(SellingRate) over (partition by RateYear) as min_rate
	, max(SellingRate) over (partition by RateYear) as max_rate, AVG(SellingRate) over (partition by RateYear) as avg_rate
		, percentile_cont(0.25) within group (order by SellingRate asc) over(partition by RateYear) as "25th_percentile"
			, percentile_cont(0.5) within group (order by SellingRate asc) over(partition by RateYear) as median_value
				, percentile_cont(0.75) within group (order by SellingRate asc) over(partition by RateYear) as "75th_percentile"
	, stdev(SellingRate) over (partition by RateYear) as standard_deviation
from [dbo].[USDExchangeRateData]
ORDER BY 1 ASC
GO
/*-- We understand there is an upward trend in the price movement of the USD year on year. However, there are outliers in our time-series data, 
that is, unusual data points either far below the 25th percentile or far above the 75th percentile which skewed the data evident from the Standard Deviation measured that year. 
These unusual data points are inconsistent with the other rates at the same point in time in our dataset. */

-- Examining the absolute deviations from the horizontal mean of typical data points
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
		, FLOOR(ABS(mid_buy-ty_avg))+FLOOR(ABS(mid_central-ty_avg))+FLOOR(ABS(mid_sell-ty_avg)) indicator
FROM (
	SELECT
		RateYear, num_of_observations, mid_buy, mid_central, mid_sell, (mid_buy+mid_central+mid_sell)/3 ty_avg
	FROM mid ) tbl
ORDER BY 1 ASC;
/*-- The result of this analysis reveal that there is "typically" no difference between the rates year on year, and further confirms that 
those unusual data points are outliers. There is therefore need to clean our dataset to rid it of these outliers. */

/*
One statistical method of identifying outliers is through the use of the interquartile range, or IQR. When we find values that fall outside of 1.5 times the range 
between our first and third quartiles, we typically consider these to be outliers.
References: 
1. https://dataschool.com/how-to-teach-people-sql/how-to-find-outliers-with-sql/
2. https://towardsdatascience.com/why-1-5-in-iqr-method-of-outlier-detection-5d07fdc82097
3. https://youtu.be/9jYqZS142mg
*/
-- Extracting the outliers
WITH BuyingOutliers AS (
	SELECT 
		t1.Id, t1.RateDate, BuyingRate, CentralRate, SellingRate, (BuyingRate + CentralRate + SellingRate)/3 avg_rate
			, (t2.q_one - outlier_range) lowerbound, (t2.q_three + outlier_range) upperbound
	FROM [dbo].[USDExchangeRateData] t1
	JOIN (
		select	
			RateYear, Rate, q_one, q_three, 1.5*(q_three - q_one) as outlier_range
		from (
			select distinct
				RateYear, 'BuyingRate' as Rate
					, percentile_cont(0.25) within group (order by BuyingRate asc) over(partition by RateYear) as q_one
							, percentile_cont(0.75) within group (order by BuyingRate asc) over(partition by RateYear) as q_three
			from [dbo].[USDExchangeRateData] ) t1 ) t2
			ON t1.RateYear = t2.RateYear AND (t1.BuyingRate < (t2.q_one - outlier_range) OR t1.BuyingRate > (t2.q_three + outlier_range))
), CentralOutliers AS (
	SELECT 
		t1.Id, t1.RateDate, BuyingRate, CentralRate, SellingRate, (BuyingRate + CentralRate + SellingRate)/3 avg_rate
			, (t2.q_one - outlier_range) lowerbound, (t2.q_three + outlier_range) upperbound
	FROM [dbo].[USDExchangeRateData] t1
	JOIN (
		select	
			RateYear, Rate, q_one, q_three, 1.5*(q_three - q_one) as outlier_range
		from (
			select distinct
				RateYear, 'CentralRate' as Rate
					, percentile_cont(0.25) within group (order by CentralRate asc) over(partition by RateYear) as q_one
							, percentile_cont(0.75) within group (order by CentralRate asc) over(partition by RateYear) as q_three
			from [dbo].[USDExchangeRateData] ) t1 ) t2
			ON t1.RateYear = t2.RateYear AND (t1.CentralRate < (t2.q_one - outlier_range) OR t1.CentralRate > (t2.q_three + outlier_range))
), SellingOutliers AS (
	SELECT 
		t1.Id, t1.RateDate, BuyingRate, CentralRate, SellingRate, (BuyingRate + CentralRate + SellingRate)/3 avg_rate
			, (t2.q_one - outlier_range) lowerbound, (t2.q_three + outlier_range) upperbound
	FROM [dbo].[USDExchangeRateData] t1
	JOIN (
		select	
			RateYear, Rate, q_one, q_three, 1.5*(q_three - q_one) as outlier_range
		from (
			select distinct
				RateYear, 'SellingRate' as Rate
					, percentile_cont(0.25) within group (order by SellingRate asc) over(partition by RateYear) as q_one
							, percentile_cont(0.75) within group (order by SellingRate asc) over(partition by RateYear) as q_three
			from [dbo].[USDExchangeRateData] ) t1 ) t2
			ON t1.RateYear = t2.RateYear AND (t1.SellingRate < (t2.q_one - outlier_range) OR t1.SellingRate > (t2.q_three + outlier_range))
)
SELECT *
INTO USDExchangeRateData_Outliers 
FROM (
	SELECT
		Id, RateDate, BuyingRate, CentralRate, SellingRate
	FROM BuyingOutliers
	WHERE (FLOOR(ABS(BuyingRate-avg_rate)) + FLOOR(ABS(CentralRate-avg_rate)) + FLOOR(ABS(SellingRate-avg_rate))) > 0 
	UNION
	SELECT
		Id, RateDate, BuyingRate, CentralRate, SellingRate
	FROM CentralOutliers
	WHERE (FLOOR(ABS(BuyingRate-avg_rate)) + FLOOR(ABS(CentralRate-avg_rate)) + FLOOR(ABS(SellingRate-avg_rate))) > 0 
	UNION
	SELECT
		Id, RateDate, BuyingRate, CentralRate, SellingRate
	FROM SellingOutliers
	WHERE (FLOOR(ABS(BuyingRate-avg_rate)) + FLOOR(ABS(CentralRate-avg_rate)) + FLOOR(ABS(SellingRate-avg_rate))) > 0
) tbl 
ORDER BY 2 ASC
GO

SELECT * FROM [dbo].[USDExchangeRateData_Outliers];
GO
-- -----------------------
-- We'll update the anomalous data points with their preceding rates
UPDATE [dbo].[ExchangeRateData]
SET BuyingRate = e2.pre_BuyingRate, CentralRate = e2.pre_CentralRate, SellingRate = e2.pre_SellingRate
FROM [dbo].[ExchangeRateData] e1
JOIN (
	SELECT
		Id, RateDate, pre_BuyingRate, pre_CentralRate, pre_SellingRate
	FROM (
		SELECT 
			Id, RateDate
			, LAG(BuyingRate, 1) OVER (ORDER BY RateDate ASC) AS pre_BuyingRate
			, LAG(CentralRate, 1) OVER (ORDER BY RateDate ASC) AS pre_CentralRate
			, LAG(SellingRate, 1) OVER (ORDER BY RateDate ASC) AS pre_SellingRate
		FROM [dbo].[USDExchangeRateData] 
	) t1
	WHERE EXISTS (
			SELECT
				1 
			FROM [dbo].[USDExchangeRateData_Outliers] t2
			WHERE t1.Id = t2.Id ) 
	) e2 ON CONCAT(e1.RateDate, e1.Currency) = CONCAT(e2.RateDate, 'US DOLLAR');
-- ---------------------
GO
DROP TABLE [dbo].[USDExchangeRateData_Outliers]
GO
SELECT * FROM [dbo].[USDExchangeRateData]
ORDER BY 1 ASC
GO

-- Descriptive statistics of the cleaned dataset
with tbl as (
	select
		row_number() over (partition by RateYear order by CentralRate asc) row_num, RateYear, CentralRate as rate
        , count(Id) over (partition by RateYear) as datapoints
			, min(CentralRate) over (partition by RateYear) as min_rate, max(CentralRate) over (partition by RateYear) as max_rate
            , AVG(CentralRate) over (partition by RateYear) as avg_rate, stdev(CentralRate) over (partition by RateYear) as standard_deviation
	from [dbo].[USDExchangeRateData]
), bounds as (
	select distinct
		RateYear, floor((max(row_num) over (partition by RateYear)-1)/2) as lowerbound, ceiling((max(row_num) over (partition by RateYear)-1)/2) as upperbound
	from tbl 
) 
select distinct RateYear, datapoints, avg(rate) over (partition by RateYear) as median_rate, avg_rate, min_rate, max_rate, standard_deviation
from tbl
where CONCAT(RateYear, row_num) in (
								select CONCAT(RateYear, lowerbound)
                                from bounds ) or 
	  CONCAT(RateYear, row_num) in (
								select CONCAT(RateYear, upperbound)
                                from bounds )
order by 1 asc;
-- The statistics of the CentralRate, which is typically homogeneous with the other rates describe our dataset to be clean and useable for analysis. 

-- The end ---------------------------------




