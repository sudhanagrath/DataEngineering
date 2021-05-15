# BUILDING DATA WAREHOUSE
This Readme is about creating a datawarehouse on the analytic platform of Redshift.This datawarehouse is created on 4 node cluster of type dc2.large size.

## INTRODUCTION
Challenging part of this project is to get connected to the Redshift cluster from my local computer and load the data from S3 buckets into redshift cluster located in the different region than the bucket. Based in UK, I have chosen to build the cluster in eu-west-2 and the S3 buckets are located in the us-west-2. After many attempts to create the cluster using Redshift GUI, decided to build it programmatically. All this experience can be grouped into the following 3 secitons:


* Building the Redshifting Cluster
* Creating the Staging tables and Analytic tables
* ETL process from S3 buckets to the Staging and then to analytic tables

### Building Redshift Cluster
Following one of the class lectures, built the redshift cluster programmatically. It is in the file built_cluster.ipynb. The access and secret key required to build the cluster are provided in the configuration file dwh.cfg. I have created a security group redshift-security-group by opening a TCP port in the cluster for the outbound internet traffic. This security group needs to be attached to the cluster as a manual step in order to be able to connect to it.

### Creating Tables
As specified in the Project instructions and Project Dataset, defined  queries for creating the staging and analytic tables. Running the script create_tables.py creates the 7 tables successfully.

### Building ETL processes
There are 2 ETL processes run by the script etl.py:

* First one copies the two S3 Buckets, 's3://udacity-dend/log-data' and 's3://udacity-dend/song-data' into staging\_events and staging_songs tables respectively.
* Second one bulk loads the two staging tables into four dimensional and one fact tables.

Providing my IAM Role as credential in the copy command kept giving me the following error message:

Traceback (most recent call last):
  File "etl.py", line 32, in <module>
    main()
  File "etl.py", line 25, in main
    load_staging_tables(cur, conn)
  File "etl.py", line 8, in load_staging_tables
    cur.execute(query)
psycopg2.errors.InternalError_: User arn:aws:redshift:eu-west-2:795712660208:dbuser:redshift-cluster-1/awsuser is not authorized to assume IAM Role  arn:aws:iam::795712660208:role/dwhRole. 

DETAIL:  
  -----------------------------------------------
  error: User 
   arn:aws:redshift:eu-west-2:795712660208:dbuser:redshift-cluster-1/awsuser is not authorized to assume IAM Role arn:aws:iam::795712660208:role/dwhRole. 
  code:      8001
  context:   IAM Role= arn:aws:iam::795712660208:role/dwhRole
  query:     561
  location:  xen_aws_credentials_mgr.cpp:450
  process:   padbmaster [pid=18017]
  -----------------------------------------------


Cause of the above error is that the redshift cluster is built in different region than that of S3 buckets.
Resolved it by replacing the role based credentials with access key id and secret. 

The next challange has been loading the staging\_songs  table from S3 bucket that contains approximately 390,000 json files to copy over to redshift. Made the following changes in the dwh.cfg and sql_queries.py to load the entire song-data dataset in the S3 bucket.

-	Turned off the default compression analysis of redshift by using the compupdate off option of the copy command in sql_queries.py
- 	Some of the files in song-data object have more than default length of user_agent column and causing the load error - Length is more than the DDL length. Added the truncatecolumn option to copy command for the staging\_songs table.
-	Trying load the song-data in one copy session, results in redshift aborting the query after 10 hours even with 4 node cluster.
-	 Loaded the song-data in 26 copy sessions by defining these many keys for the s3_song_data session in the dwh.cfg file. 

The total time and number of rows processed by the COPY processes are:

staging\_events:
Total number of rows loaded:8056
Total time: less than 5 seconds

staging_songs:
Total number of rows= 296134
Total time to load:18 min for one copy session * 26 sessions=~7 hours
The total number of rows processed by the BULK Load are:

users: 105

songs:295895

artists:40506

time :16046

songplays:5115







