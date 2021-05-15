This Readme is about building Data Lake project using Apache Spark.

There are 3 files including this Readme:

dl.cfg - configuration file containing AWS key Id and Acces key to provide to the spark context for creating session.

etl.py - ETL script that reads from an s3 bucket, defines analytic tables using Spark SQL and write them back to another s3 bucket for the analytic team of the Sparkify company. I have tested this script on my computer creating Spark session in the local mode. 

It was interesting to monitor the SparkUI on http://localhost:4040 for the jobs being run for reading and writing the files. As expected the script takes much longer time than the etl_emr.py, particularly for song_data files. Following is the list for some of the oberservations:
There are in total 14896 paths for the directories and leaf files for the song_data json files to read.
It takes 14 minutes in 2 stages for a Spark job to list those directories and files for reading, making it more than 30 minutes to list them.
After the listing job has completed, json reading job kicks off which reads 466 objects in one stage. 

Finally, parquet job that writes the spark data frames to datalake2 bucket starts up in Stage 3 for all those 466 objects.

I landed up running etl.py locally over 12 hours, still couldn't complete writing all the songs data.

Name of the bucket: datalake2  created in us-west-2

This scripts writes following five parquet folders under the output_data folder of the datalake2 bucket.

* songs_parquet
* artists_parquet
* users_parquet
* time_parquet
* songplays_parquet

Following are the steps I have taken to deploy the script on EMR cluster.

*  Create the cluster version 6.2.0 in order to use Spark SQL 3.0.1

aws emr create-cluster --name emr-cluster-1 --use-default-roles --release-label emr-6.2.0 --instance-count 3 --applications Name=Spark  --ec2-attributes KeyName=ec2_key --instance-type m5.xlarge --instance-count 3 

* Generate SSH key pair and upload its public key to the EC2 dashboard. Name of the key is ec2_key.

* Check if the cluster is getting created without any errors.
aws emr describe-cluster --cluster-id 

* Copy the private key over to Master node.

scp -i  /Users/sudhanagrath/.ssh/ec2_key /Users/sudhanagrath/.ssh/ec2_key  hadoop@ec2-44-234-42-153.us-west-2.compute.amazonaws.com:/home/hadoop/

* Copy the private dl.cfg key over to Master node.

scp -i  /Users/sudhanagrath/.ssh/ec2_key dl.cfg  hadoop@ec2-44-234-42-153.us-west-2.compute.amazonaws.com:/home/hadoop/

* Copy the etl.py script over to Mast node.

scp -i  /Users/sudhanagrath/.ssh/ec2_key etl.py hadoop@ec2-44-234-42-153.us-west-2.compute.amazonaws.com:/home/hadoop/

* Connect to the Maste node.

ssh -i ~/.ssh/ec2_key hadoop@ec2-44-234-42-153.us-west-2.compute.amazonaws.com

* Run the script on EMR cluster.

/usr/bin/spark-submit --master yarn etl.py

It ran way faster here than it ran in local mode.


Troubleshooting:
Both the input and output buckets should be in the same region, otherwise Spark job for writing throws the following error:
raise Py4JJavaError(
py4j.protocol.Py4JJavaError: An error occurred while calling o47.parquet.
: com.amazonaws.services.s3.model.AmazonS3Exception: Status Code: 400, AWS Service: Amazon S3, AWS Request ID: 9JDFM4CXJB9HHS7T, AWS Error Code: null, AWS Error Message: Bad Request