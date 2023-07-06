## Building a Data Pipeline with an Open Source Stack
Previously on the following [Azure Databricks - Data Pipeline](https://medium.com/@alonso.md/building-an-end-to-end-data-pipeline-using-azure-databricks-part-1-82ad28a8f8f) article I present how to build and end to end data pipeline using Azure Databricks and Data Factory. Now I'm doing a similar implementation using an open-source stack leveraging some of the best existing solutions:

- **Airbyte:** data extraction - http://localhost:56174/
- **Dbt:** data transformation
- **Airflow:** task orchestration - http://localhost:8085/
- **PostgreSQL:** data storage
- **Openmetadata:** data catalog - http://localhost:8585/
- **Minio:** object storage - http://localhost:9090/

## Use Case Explanation
We will be working with transactional data referred to loan transactions and customers from GeekBankPE (a famous bank around the world).

You have two requirements from different areas of the bank.

- The Marketing area needs to have updated customer data to be able to contact them and make offers.
- The Finance area requires to have daily loan transactions complemented with customer drivers to be able to analyze them and improve the revenue.

To fulfill with the request, we are going to perform incremental loads and also using techniques like upsert.

--- 

## Requirements
- Docker Desktop
- Docker compose
- Windows 10 or higher (**This project hasn't been tested on MacOS or Linux**)
- Python 3.6 or higher
- Recommended 8GB RAM or higher only for docker containers.

---

## Credentials

### Airflow
- **username:** `airflow`
- **password:** `airflow`

### Minio
Minio acces and secret keys along with url_endpoint are used to log in and for connections.
- **minio_access_key:** `minio_admin`
- **minio_secret_key:** `minio_password`
- **url_endpoint:** http://host.docker.internal:9000
- **port:** 9000

### Postgres DWH
- **username:** `dwh`
- **password:** `dwh`
- **external_host:** `local_host`
- **external_port:** 5455
- **internal_host:** `postgres_dwh`
- **interal_port:** `5432`
*If you connect to your database through Mysql or DBeaver, you need to use the external host and port.*

### Airbyte
Enter a valid email when trying to log in.
- For other configurations:
 - **internal_host:** `host.docker.internal`
 - **internal_host_and_port:** `http://host.docker.internal:8000`
 - **user:** `airbyte`
 - **password:** `password`

### Openmetadata
- **username:** `admin`
- **password:** `admin`

---

## Youtube Video
In the below youtube video, I show you how to set up everything.
- [Open Source Data Stack](https://www.youtube.com/watch?v=Ncgyg_wxEy4)

## Setup Instructions
1. Open your terminal.
2. Navigate to the root of the `os-data-stack` repository
3. Run `docker compose up --build` to initialize the containers. If you want to run it in the background, add `-d` argument.
4. Perform Airflow configurations (*Section below*)
5. Run the Airflow DAG called `upload_data_to_s3` for uploading csv files into minio s3 bucket.
6. Perform Airbyte configurations (*Section below*)
7. Run the Airflow DAG called `dbt_model` to create the tables in the bronze, silver, and gold schemas.
8. Perform Openmetadata configurations. (*Section below*)
9. Play around all the technologies.

---

## Airflow Configurations
1. Open Airflow
2. Go to connections
3. Create the Minio S3 connection with the below configuration:
*If you test this connection it will fail, just ignore it.*
    - **Connection Type:** `Amazon Web Services`
    - **Connection Id:** `aws_default`
    - **Extra:** `{"aws_access_key_id": "minio_admin", "aws_secret_access_key": "minio_password", "endpoint_url": "http://host.docker.internal:9000"}`
4. Create the Postgres connection
    - **Connection Type:** `Postgres`
    - **Connection Id:** `postgres_default`
    - **Host:** `postgres_dwh`
    - **Schema:** `dwh`
    - **Login:** `dwh`
    - **Password:** `dwh`
    - **Port:** `5432`
5. Create the Airbyte connection (Optional, in case you want to use the stack for your own development)
    - **Connection Type:** `Airbyte`
    - **Connection Id:** `airbyte_default`
    - **Host:** `host.docker.internal`
    - **Username:** `airbyte`
    - **Password:** `password`
    - **Port:** `8000`

---

## Airbyte Configurations
1. Open Airbyte, enter an email and select `Get started`
2. Select sources (*left sidebar*) , in the search bar write `S3` and select it 
3. Create the S3 connection for customer data
    - **Source_name:** `S3_customer_information_cdc`
    - **Output_stream_name:** `daily_customer_information_cdc`
    - **Pattern_of_files_to_replicate:** `customer/*.csv`
    - **Bucket:** `raw`
    - **Aws_access_key_id:** `minio_admin`
    - **Aws_secret_access_key:** `minio_password`
    - **Path_prefix:** `customer/`
    - **Endpoint:** `http://host.docker.internal:9000`
    - ***Scroll until the end and select `set up source`***
4. Create the S3 connection for customer driver data
    - **Source_name:** `S3_daily_customer_drivers`
    - **Output_stream_name:** `daily_customer_drivers`
    - **Pattern_of_files_to_replicate:** `customerDrivers/*.csv`
    - **Bucket:** `raw`
    - **Aws_access_key_id:** `minio_admin`
    - **Aws_secret_access_key:** `minio_password`
    - **Path_prefix:** `customerDrivers/`
    - **Endpoint:** `http://host.docker.internal:9000`
    - ***Scroll until the end and select `set up source`***
5. Create the S3 connection for loan transactions data
    - **Source_name:** `S3_daily_loan_transactions`
    - **Output_stream_name:** `daily_loan_transactions`
    - **Pattern_of_files_to_replicate:** `transactions/*.csv`
    - **Bucket:** `raw`
    - **Aws_access_key_id:** `minio_admin`
    - **Aws_secret_access_key:** `minio_password`
    - **Path_prefix:** `transactions/`
    - **Endpoint:** `http://host.docker.internal:9000`
    - ***Scroll until the end and select `set up source`***
5. Select Destinations (*left sidebar*), search for Postgres and select it.
6. Create the Postgres connection as destination
    - **Destination_name:** `Postgres_DWH`
    - **Host:** `localhost`
    - **Port:** `5455`
    - **Db_name:** `dwh`
    - **Default_Schema**: `airbyte`
    - **user:** `dwh`
    - **password:** `dwh`
    - ***Scroll until the end and select `set up destination`***
7. Select Connections (*left sidebar*)
    - Select the `S3_customer_information_cdc` source
    - Select `Use existing destination`
    - In the destination tab select **Postgres_DWH** and select `Use existing destination`
    - In the new screen view, change the `Replication frequency` to `Manual`
    - Sync mode should be `Full refresh overwrite` (*2023-05-01 - Incremental sync mode isn't  working, data gets duplicated when using it, maybe because the Postgres connector is in Alpha*)
    - Select `set up connection`
    - Repeat the same process for `S3_daily_customer_drivers` and `S3_daily_loan_transactions` sources.
8. After setting up the connections, select Connections (*left side bar*) and click on `Sync now` for your 3 connections.
9. Wait until the syncronization finished.

---

## Openmetadata Configurations

### Database Services - Postgres
1. Open openmetadata, enter your credentials
    - **username:** `admin`
    - **password:** `admin`
2. Select `Services` in the left sidebar.
3. Select `Add New Service` in the right top corner.
4. Create the Postgres database service.
    - Select Database Services.
    - Select the **Postgres connector** and select `Next`
    - Enter `postgres_con` as service name and select `Next`
    - Fill out the fields for the connection:
        - **Username:** `dwh`
        - **Password:** `dwh`
        - **Host_and_Port:** `postgres_dwh`
        - **Database:** `dwh`
    - Test your connection and select `save`
5. Select your connection.
6. Select the tab called `Ingestions`
7. Create a metadata ingestion by selecting the `Add ingestion` purple button on the right **Mandatory**
    - Enable `Use FQN For Filtering`
    - Enable `Include Views`
    - Select `Next`
    - Schedule interval, select `None`, which means Manual.
    - Finally select `Add & Deploy` `and then View Service`
    - Run the metadata ingestion.
8. Create a DBT Ingestion **Mandatory  (For our example)**
    - **dbt_Configuration_Source:** `S3_Config_Source`
    - **AWS_Access_Key_ID**: `minio_admin`
    - **AWS_Secret_Access_Key**: `minio_password`
    - **AWS_Region**: `us-east-1`
    - **Endpoint_URL**: `http://host.docker.internal:9000`
    - **dbt_Bucket_Name**: `dbt`
    - **dbt_Object_Prefix**: `dwh`
    - Select `Next`, choose a `Manual` schedule interval.
    - Run the DBT ingestion
9. Create Lineage Ingestion **Optional**
9. Create a Profiler Ingestion **Optional**
    - Recommended to filter schema and target only: `bronze,silver,gold`
    - Recommended to play with the value of `Profile Sample Type` 
10. Create a Usage Ingestion **Optional**

### Pipeline Services - Airflow
1. Select `Services` in the left sidebar.
2. Select `Add New Service` in the right top corner.
3. Select `Pipeline Services` from the drop down list.
4. Select `Airflow` and then `Next`
5. Enter a name for your service connection and select `Next`
6. Enter the below configuration:
    - **Host_and_Port:** `http://localhost:8085/`
    - **Metadata_Database_Connection:** `PostgresConnection`
    - **Username:** `airflow`
    - **Password:** `airflow`
    - **Host_and_Port:** `host.docker.internal:5432`
    - **Database:** `airflow`
7. Test your connection and save.
8. Navigate to your airflow service connection and create a metadata ingestion. 
9. Run the metadata ingestion.

### Pipeline Services - Airbyte
1. Select `Services` in the left sidebar.
2. Select `Add New Service` in the right top corner.
3. Select `Pipeline Services` from the drop down list.
4. Select `Airbyte` and then `Next`
5. Enter a name for your service connection and select `Next`
6. Enter the below configuration:
    - **Host_and_Port:** `http://host.docker.internal:8000`
    - **Metadata_Database_Connection:** `PostgresConnection`
    - **Username:** `airbyte`
    - **Password:** `password`
7. Test your connection and save.
8. Navigate to your airflow service connection and create a metadata ingestion. 
9. Run the metadata ingestion.

---

## How to run DBT alone
*In case you want to do some dbt actions*
1. Navigate to the root of the `dbt` directory
2. Install a python virtual environment by running:
    - Windows: `python -m venv env`
3. Activate your environment by running:
    - Windows: `env/scripts/activate`
4. Run the below command to install dbt:
    - Windows: `pip install dbt-postgres==1.4.6`
5. Navigate to the `dwh` project by running:
    - cd `dwh`
6. Now you are able to run dbt commands, follow example below:
 - `dbt build --profiles-dir "../" --target prod_localhost --vars '{ target_date: 2022-09-12 }' --select gold` 
 - `dbt test --profiles-dir "../" --target prod_localhost --vars '{ target_date: 2022-09-12 }' --select gold`
**Note: Some of the tasks will be marked as `ERROR` when running a DBT command  because data is already loaded.**
