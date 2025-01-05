--credentials creation using managed identity 
CREATE DATABASE SCOPED CREDENTIAL cred_vinay
WITH
    IDENTITY = 'Managed Identity'

--silver container data source
CREATE EXTERNAL DATA SOURCE source_silver_vinay
WITH
(
    LOCATION = 'https://yourstorageaccount.dfs.core.windows.net/silver',
    CREDENTIAL = cred_vinay
)

--gold container
--drop EXTERNAL TABLE gold.extsales;
--DROP external data source source_gold_vinay;
CREATE EXTERNAL DATA SOURCE source_gold_vinay
WITH
(
    LOCATION = 'https://yourstorageaccount.dfs.core.windows.net/gold',
    CREDENTIAL = cred_vinay
)

--Create external file format
CREATE EXTERNAL FILE FORMAT format_parquet_vinay
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'

)
/* after running the above commands you will be able to see the data sources in resource section
 specifically external data sources part of data tab, after this we have to create external tables.
 using CETAS, CETAS Stands for "create external table as Select"
*/

-----------------------------------
-- Create External Tables EXTSALES
-----------------------------------

CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold_vinay,
    FILE_FORMAT = format_parquet_vinay
)
AS
SELECT * FROM gold.sales;

/*
After running the above commands successully completing the configurations, you will be able to see
the tables gets created in your adlsgen2 (datalake) with the same table or folder name that you mentioned above.
*/


-- CREATE EXTERNAL TABLE gold.extcalendar
---------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE gold.extcalendar
WITH
(
    LOCATION = 'extcalendar',
    DATA_SOURCE = source_gold_vinay,
    FILE_FORMAT = format_parquet_vinay
)
AS
SELECT * FROM gold.calendar;



-- CREATE EXTERNAL TABLE gold.extcustomer
---------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE gold.extcustomer
WITH
(
    LOCATION = 'extcustomer',
    DATA_SOURCE = source_gold_vinay,
    FILE_FORMAT = format_parquet_vinay
)
AS
SELECT * FROM gold.customer;


-- CREATE EXTERNAL TABLE gold.extprocat --product categories
---------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE gold.extprocat
WITH
(
    LOCATION = 'extprocat',
    DATA_SOURCE = source_gold_vinay,
    FILE_FORMAT = format_parquet_vinay
)
AS
SELECT * FROM gold.procat;



-- CREATE EXTERNAL TABLE gold.extproduct
---------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE gold.extproduct
WITH
(
    LOCATION = 'extproduct',
    DATA_SOURCE = source_gold_vinay,
    FILE_FORMAT = format_parquet_vinay
)
AS
SELECT * FROM gold.product;


-- CREATE EXTERNAL TABLE gold.extreturns
---------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE gold.extreturns
WITH
(
    LOCATION = 'extreturns',
    DATA_SOURCE = source_gold_vinay,
    FILE_FORMAT = format_parquet_vinay
)
AS
SELECT * FROM gold.returns;


-- CREATE EXTERNAL TABLE gold.extsubcat --sub categories
---------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE gold.extsubcat
WITH
(
    LOCATION = 'extsubcat',
    DATA_SOURCE = source_gold_vinay,
    FILE_FORMAT = format_parquet_vinay
)
AS
SELECT * FROM gold.subcat;


-- CREATE EXTERNAL TABLE gold.extterritories
---------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE gold.extterritories
WITH
(
    LOCATION = 'extterritories',
    DATA_SOURCE = source_gold_vinay,
    FILE_FORMAT = format_parquet_vinay
)
AS
SELECT * FROM gold.territories;