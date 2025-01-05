-- Azure Synapse Analytics SQL Scripts

-- Schema Creation
CREATE SCHEMA gold;

-- CREATE VIEW calendar
---------------------------------------------------------------------------------------------------
CREATE VIEW gold.calendar AS
SELECT *
 FROM
    OPENROWSET(
        BULK  'https://yourstorageaccount.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
        /*since our data is in datalake we have replaced the value in above link from blob to dfs
        What do we mean by OPENROWSET function in SQL. 
        */
        FORMAT = 'PARQUET'
    )as query1;  


-- CREATE VIEW customer
---------------------------------------------------------------------------------------------------
CREATE VIEW gold.customer AS
SELECT *
 FROM
    OPENROWSET(
        BULK  'https://yourstorageaccount.dfs.core.windows.net/silver/AdventureWorks_Customers/',
        /*since our data is in datalake we have replaced the value in above link from blob to dfs
        What do we mean by OPENROWSET function in SQL. 
        */
        FORMAT = 'PARQUET'
    )as query1;


-- CREATE VIEW product
---------------------------------------------------------------------------------------------------
CREATE VIEW gold.product AS
SELECT *
 FROM
    OPENROWSET(
        BULK  'https://yourstorageaccount.dfs.core.windows.net/silver/AdventureWorks_Products/',
        /*since our data is in datalake we have replaced the value in above link from blob to dfs
        What do we mean by OPENROWSET function in SQL. 
        */
        FORMAT = 'PARQUET'
    )as query1;  


-- CREATE VIEW Product_Categories
---------------------------------------------------------------------------------------------------
CREATE VIEW gold.procat AS
SELECT *
 FROM
    OPENROWSET(
        BULK  'https://yourstorageaccount.dfs.core.windows.net/silver/AdventureWorks_Product_Categories/',
        /*since our data is in datalake we have replaced the value in above link from blob to dfs
        What do we mean by OPENROWSET function in SQL. 
        */
        FORMAT = 'PARQUET'
    )as query1;   


-- CREATE VIEW Product_Categories
---------------------------------------------------------------------------------------------------
CREATE VIEW gold.procat AS
SELECT *
 FROM
    OPENROWSET(
        BULK  'https://yourstorageaccount.dfs.core.windows.net/silver/AdventureWorks_Product_Categories/',
        /*since our data is in datalake we have replaced the value in above link from blob to dfs
        What do we mean by OPENROWSET function in SQL. 
        */
        FORMAT = 'PARQUET'
    )as query1;


-- CREATE VIEW returns
---------------------------------------------------------------------------------------------------
CREATE VIEW gold.returns AS
SELECT *
 FROM
    OPENROWSET(
        BULK  'https://yourstorageaccount.dfs.core.windows.net/silver/AdventureWorks_Returns/',
        /*since our data is in datalake we have replaced the value in above link from blob to dfs
        What do we mean by OPENROWSET function in SQL. 
        */
        FORMAT = 'PARQUET'
    )as query1;  


-- CREATE VIEW sales
---------------------------------------------------------------------------------------------------
CREATE VIEW gold.sales AS
SELECT *
 FROM
    OPENROWSET(
        BULK  'https://yourstorageaccount.dfs.core.windows.net/silver/AdventureWorks_Sales/',
        /*since our data is in datalake we have replaced the value in above link from blob to dfs
        What do we mean by OPENROWSET function in SQL. 
        */
        FORMAT = 'PARQUET'
    )as query1;


-- CREATE VIEW subcat
---------------------------------------------------------------------------------------------------
CREATE VIEW gold.subcat AS
SELECT *
 FROM
    OPENROWSET(
        BULK  'https://yourstorageaccount.dfs.core.windows.net/silver/AdventureWorks_Product_Subcategories/',
        /*since our data is in datalake we have replaced the value in above link from blob to dfs
        What do we mean by OPENROWSET function in SQL. 
        */
        FORMAT = 'PARQUET'
    )as query1;  


-- CREATE VIEW territories
---------------------------------------------------------------------------------------------------
CREATE VIEW gold.territories AS
SELECT *
 FROM
    OPENROWSET(
        BULK  'https://yourstorageaccount.dfs.core.windows.net/silver/AdventureWorks_Territories/',
        /*since our data is in datalake we have replaced the value in above link from blob to dfs
        What do we mean by OPENROWSET function in SQL. 
        */
        FORMAT = 'PARQUET'
    )as query1;