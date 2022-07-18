-- The median of a dataset
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

-- The difference between PERCENTILE_CONT and PERCENTILE_DISC
/* The continuous model interpolates based on the values in the dataset, while the discrete model
returns the value at the position where 50% of the values either falls below or above in the ordered dataset */
with base as (
    select
        level as x
    from dual
    connect by level <= 10
)
select 
    x, percentile_cont(0.5) within group (order by x asc) over() as pcont_median
        , percentile_disc(0.5) within group (order by x asc) over() as pdisc_median
from base
order by 1 asc;

