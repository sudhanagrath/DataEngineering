# Environment
This Readme is about environment for the data pipeline.

The three scripts for pipeline, create_tables.py, sql_queries.py and etl.py are developed and tested on my local machine. So I have used my local environment. Please replace the following paths in main methods of the scripts:

* PATH

* JAVA_HOME

* SPARK_HOME

The code in these scripts is based on the latest version of the Spark  and the SAS module.

The scripts should be executed in the following order:

* python create_tables.py

* python etl.py

python quality_checks.py

# Script Code

Drop queries and create queries with export prefix refers to the HIVE internal tables. i.e  the meta data exists in the metastore_db directory and data exists in the local spark-warehouse directory. These queries just drops and creates the tables for SAS data.

Drop queries and create queries with import prefix refers to the HIVE external tables. i.e. the meta data exists in the metastore_db directory but the table data is located in the AWS S3 bucket. Folders with the specified table should exists in the bucket before executing the ETL scripts.

Dimensions for processing SAS data and the US-demograhics.csv are created and loaded by the insert_table_csv_queries using CSV schemas by etl.py script.

quality_checks.py just counts the number of records in the tables.


