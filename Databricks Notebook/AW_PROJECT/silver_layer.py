# Databricks notebook source
# MAGIC %md
# MAGIC ### **IMPORTING PySpark Libraries And Functions**

# COMMAND ----------

# Please replace your below configurations before running this below databricks notebook
# yourstorageaccount, yourapplicationID, yoursecretkeygenerated, yourtenenatID

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC # SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA ACCESS USING APPLICATION

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.yourstorageaccount.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.yourstorageaccount.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.yourstorageaccount.dfs.core.windows.net", "yourapplicationID")
spark.conf.set("fs.azure.account.oauth2.client.secret.yourstorageaccount.dfs.core.windows.net", "yoursecretkeygenerated")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.yourstorageaccount.dfs.core.windows.net", "https://login.microsoftonline.com/yourtenenatID")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA LOADING

# COMMAND ----------

# MAGIC %md
# MAGIC **READING DATA**

# COMMAND ----------

df_cal = spark.read.format("csv")\
                .option("header", True)\
                .option("inferSchema", True)\
                .load('abfss://bronze@yourstorageaccount.dfs.core.windows.net/AdventureWorks_Calendar')


# COMMAND ----------

df_cust = spark.read.format("csv")\
                .option("header",True)\
                .option("inferschema",True)\
                .load('abfss://bronze@yourstorageaccount.dfs.core.windows.net/AdventureWorks_Customers')
                

# df_cust.display()

# COMMAND ----------

df_pro_cat = spark.read.format("csv")\
                  .option("header",True)\
                  .option("inferSchema",True)\
                  .load('abfss://bronze@yourstorageaccount.dfs.core.windows.net/AdventureWorks_Product_Categories')
                                   
# df_pro_cat.display()

# COMMAND ----------

df_pro = spark.read.format("csv")\
                .option("header",True)\
                .option("inferSchema",True)\
                .load('abfss://bronze@yourstorageaccount.dfs.core.windows.net/AdventureWorks_Products')

# df_pro.display()

# COMMAND ----------

df_ret = spark.read.format("csv")\
                .option("header",True)\
                .option("inferSchema",True)\
                .load('abfss://bronze@yourstorageaccount.dfs.core.windows.net/AdventureWorks_Returns')

# df_ret.display()

# COMMAND ----------

df_sales = spark.read.format("csv")\
                .option("header",True)\
                .option("inferSchema",True)\
                .load('abfss://bronze@yourstorageaccount.dfs.core.windows.net/AdventureWorks_Sales*')

# df_sales.display()

# In the above command we are using wildcard characters, what we are saying is to read all the files which is having the naming convention as "AdventureWorks_Sales" and this is often know as using wildcard characters and "*" is being added so that it reads all the files whether it is 2015,2016 or there can be n number of files 


# COMMAND ----------

df_ter = spark.read.format("csv")\
                .option("header",True)\
                .option("ingereSchema",True)\
                .load('abfss://bronze@yourstorageaccount.dfs.core.windows.net/AdventureWorks_Territories')

# df_ter.display()

# COMMAND ----------

df_sub_cat = spark.read.format("csv")\
                .option("header",True)\
                .option("inferSchema",True)\
                .load('abfss://bronze@yourstorageaccount.dfs.core.windows.net/Product_Subcategories')

# df_sub_cat.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **TRANFORMATIONS**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calendar

# COMMAND ----------

df_cal = df_cal.withColumn('month', month(col('Date')))\
                .withColumn('year',year(col('Date')))
# df_cal.display()

# COMMAND ----------

df_cal.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@yourstorageaccount.dfs.core.windows.net/AdventureWorks_Calendar")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer

# COMMAND ----------

df_cust = df_cust.withColumn("FullName",concat(col('Prefix'),lit(' '),col('FirstName'),lit(' '),col('LastName') ))
df_cust.display()
# FullName is new the column name that you wanted to give 

# COMMAND ----------

df_cust = df_cust.withColumn("FullName",concat_ws(' ',col('Prefix'),col('FirstName'),col('LastName') ))
df_cust.display()

# COMMAND ----------

df_cust.write.format('parquet')\
        .mode('append')\
        .option("path","abfss://silver@yourstorageaccount.dfs.core.windows.net/AdventureWorks_Customers")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sub Categories

# COMMAND ----------

df_sub_cat.write.format('parquet')\
        .mode('append')\
        .option("path","abfss://silver@yourstorageaccount.dfs.core.windows.net/AdventureWorks_Product_Subcategories")\
        .save()



# COMMAND ----------

# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro = df_pro.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
                .withColumn('ProductName',split(col('ProductName'),' ')[0])


df_pro.display()

# COMMAND ----------

df_pro.write.format('parquet')\
        .mode('append')\
        .option("path","abfss://silver@yourstorageaccount.dfs.core.windows.net/AdventureWorks_Products")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Categories

# COMMAND ----------

df_pro_cat.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@yourstorageaccount.dfs.core.windows.net/AdventureWorks_Product_Categories")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Returns

# COMMAND ----------

df_ret.write.format('parquet')\
        .mode('append')\
        .option("path","abfss://silver@yourstorageaccount.dfs.core.windows.net/AdventureWorks_Returns")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Territories

# COMMAND ----------

df_ter.write.format('parquet')\
        .mode('append')\
        .option("path","abfss://silver@yourstorageaccount.dfs.core.windows.net/AdventureWorks_Territories")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate',to_timestamp('StockDate'))\
                    .withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))\
                    .withColumn('multiply',col('OrderQuantity')*col('OrderLineItem'))

                               
df_sales.display()

# COMMAND ----------

df_sales.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@yourstorageaccount.dfs.core.windows.net/AdventureWorks_Sales")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Analysis

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber')).alias('Total_Order').display()

# COMMAND ----------

df_pro_cat.display()

# COMMAND ----------

df_ter.display()