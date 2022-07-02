# About the Project
This is a data cleaning project, and the data to be cleansed is the Exchange Rate Data scraped from [CBN's official website](https://www.cbn.gov.ng/rates/ExchRateByCurrency.asp). The dataset was scraped on 6/23/2022. 

This purpose of this project is to demonstrate how data can be transformed from dirty to clean using SQL. 

## Loading the dataset
_Below are steps to importing the dataset into your database._

1. Download the scraped dataset as of 23rd of June 2022 from [OneDrive](https://1drv.ms/u/s!AhsjsqVtnlTXjl59gIsEiQqZlyR3?e=d4mkpr)
2. Execute a Create Database statement on Micrsoft SQL Server Management studio
   ```sh
   CREATE DATABASE ExampleDatabase;
   ```
3. Execute a Create Table Statement 
   ```sh
   CREATE TABLE [dbo].[ExchangeRateData](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[RateDate] [date] NOT NULL,
	[Currency] [nvarchar](50) NOT NULL,
	[RateYear] [smallint] NOT NULL,
	[RateMonth] [nvarchar](50) NOT NULL,
	[BuyingRate] [decimal](18, 2) NOT NULL,
	[CentralRate] [decimal](18, 2) NOT NULL,
	[SellingRate] [decimal](18, 2) NOT NULL,
    PRIMARY KEY CLUSTERED 
    (
      [Id] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
    ) ON [PRIMARY]
    GO
   ```
4. Execute the INSERT Script.

###### Alternatively, you can execute the transact script as a single batch which is the dataset downloaded from OneDrive. 

## Concepts covered in this project
1. Joins
2. Temp tables
3. Window functions
4. Aggregations
5. Descriptive statistics
6. Subqueries


## References
* https://dataschool.com/how-to-teach-people-sql/how-to-find-outliers-with-sql/
* https://towardsdatascience.com/why-1-5-in-iqr-method-of-outlier-detection-5d07fdc82097
* https://youtu.be/9jYqZS142mg
* https://www.cbn.gov.ng/rates/ExchRateByCurrency.asp
* https://machinelearningmastery.com/how-to-use-statistics-to-identify-outliers-in-data/
* https://towardsdatascience.com/3-reasons-to-use-views-instead-of-tables-in-power-bi-272fb9616691

