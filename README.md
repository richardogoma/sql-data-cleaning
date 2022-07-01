# About the Project
This is a data cleaning project, and the data to be cleansed is the Exchange Rate Data scraped from [CBN's official website](https://www.cbn.gov.ng/rates/ExchRateByCurrency.asp). The dataset was scraped from their website on 6/23/2022. 

This project demonstrates how data can be transformed from dirty to clean using SQL.

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

