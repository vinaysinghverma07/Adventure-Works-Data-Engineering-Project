# Adventure-Works-Data-Engineering-Project

## Description
This project demonstrates a data engineering pipeline for handling Adventure-works-datasets. The pipeline leverages Azure services like **Azure Data Factory (ADF)**, **Azure Data Lake Storage Gen2 (ADLS Gen2)**, **Databricks** & **Azure Synapse Analytics** for seamless ingestion, transformation and storage of data in **bronze, silver, and Gold** layers (Using Medallion Architecture) and creating views/tables. The processed data can then be used for advanced analytics and reporting.

---
## Features
- Automated data ingestion from Github public repository datasets via Azure Data Factory (ADF) into **bronze layer**.
- Data transformation and processing in Databricks, storing the refined data in the **silver layer**.
- Advanced transformations and reporting-ready datasets in the gold layer using Azure Synapse Analytics.
- Creation of views and external tables in Synapse Analytics for efficient querying and integration with reporting tools.

---

## Architecture
Below is the high-level architecture of the pipeline:

1. **Data Ingestion**
    - Data is fetched dynamically from the GitHub public repository using Azure Data Factory (ADF).
    - Raw data is stored in the bronze layer of ADLS Gen2 for further processing.

2. **Data Transformation**:
   - Data from the bronze layer is cleansed and refined using Databricks, with intermediate outputs stored in the silver layer of ADLS Gen2.

3. **Final Transformation and reporting**:
   - Synapse Analytics integrates with the silver layer Azure Datalakegen2 (adlsgen2) to create the gold layer, organizing data for reporting and dashboarding.
   - Views, credentials, and external tables are configured to enable seamless querying of gold layer data.


# Final Step: Configuring Synapse Analytics
  ## Creating Views
  
  - In the gold layer, we use Synapse Analytics to create views for accessing data stored in the silver layer. Below is an example of SQL used in Synapse:
  
  ### Creating Views in Synapse Analytics  

    '''sql
        CREATE VIEW gold.customer AS  
        SELECT *  
        FROM OPENROWSET(  
            BULK '[https://yourexternalstorageaccount.dfs.core.windows.net/silver/AdventureWorks_Customers/]',  
            FORMAT = 'PARQUET'  
        ) AS query1;
    '''
  
  ### Explanation:

    CREATE VIEW: Defines a view named gold.customer.
    OPENROWSET: Enables querying external data directly from Azure Data Lake.
    BULK: Specifies the path to the data file in the silver layer (replace with your data path), in our case it is awstoragedatalakevinay.
    FORMAT = 'PARQUET': Indicates the file format being queried is Parquet.
    
  This view allows seamless querying of external data, bridging the silver and gold layers for analysis and reporting.

  ## Creating Credentials and External Data Sources

  To access and manage data securely, we create credentials and external data sources:

  ### Step 1: Create Database Scoped Credential

    '''sql
        CREATE DATABASE SCOPED CREDENTIAL cred_vinay  
        WITH IDENTITY = 'Managed Identity';
      '''
  
  ### Step 2: Create External Data Sources

  For ***Silver Layer***:

    '''sql
        CREATE EXTERNAL DATA SOURCE source_silver_vinay  
        WITH (  
            LOCATION = 'https://yourexternalstorageaccount.dfs.core.windows.net/silver',  
            CREDENTIAL = cred_vinay  
        );
    '''
  
  For ***gold layer***:

    '''sql
        CREATE EXTERNAL DATA SOURCE source_gold_vinay  
        WITH (  
            LOCATION = 'https://yourexternalstorageaccount.dfs.core.windows.net/gold',  
            CREDENTIAL = cred_vinay  
        );
    '''

  ### Creating External File Format

    '''sql
        CREATE EXTERNAL FILE FORMAT format_parquet_vinay  
        WITH (  
            FORMAT_TYPE = PARQUET,  
            DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'  
        );
    '''
  
  ### Creating External Tables (CETAS)
  CETAS (Create External Table As Select) enables you to create tables directly in the gold layer:
  Example: Creating External Table for Sales Data

    '''sql
          CREATE EXTERNAL TABLE gold.extsales  
          WITH (  
              LOCATION = 'extsales',  
              DATA_SOURCE = source_gold_vinay,  
              FILE_FORMAT = format_parquet_vinay  
          )  
          AS  
          SELECT * FROM gold.sales;
    '''


## Key Learnings
- Medallion Architecture: Organized data into bronze, silver, and gold layers for better manageability.
- Secure Access: Used managed identities and credentials for secure integration between Synapse Analytics and ADLS Gen2.
- Scalable Querying: Leveraged views, external tables, and Parquet file formats for high-performance data queries.

## Technologies Used
- Azure Data Factory (ADF)
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Databricks (PySpark)
- Azure Synapse Analytics
- SQL

## Getting Started

### Prerequisites
- Azure Subscription with access to:
  - Azure Data Factory
  - Azure Data Lake Storage Gen2
  - Azure Databricks Workspace
  - Azure Synapse Analytics
- Git installed locally

### Installation
1. Clone this repository:
   ```bash
   git clone https://github.com/vinaysinghverma07/Adventure-Works-Data-Engineering-Project.git

---

### **Why This Structure Works for Your Case**:
1. **Description**: Tailored to your actual architecture using ADF, ADLS Gen2, Databricks and Azure Synapse Analytics.
2. **Features**: Focuses on the Azure & Adventure works dataset ingestion and transformation workflow.
3. **Architecture**: Highlights the layers (Medallion architecture (bronze, silver, Gold)) and the tools used.
4. **Usage**: Clear steps for setting up and running the pipeline.


Let me know if you need further modifications!
