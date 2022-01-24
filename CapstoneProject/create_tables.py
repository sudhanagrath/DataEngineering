import configparser
import os
from pyspark.sql import SparkSession
from sql_queries import  drop_table_queries, create_table_queries

def create_spark_session():
    """
       Creates a session for the Spark SQL Application using Builder method of the SparkSession module.
       First config option downloads the Apache web module and executes the maven task to store its jar packages under local directory ~/ivy2
       getOrCreate() method create a session if it does not exist already
       This Spark session is enabled for the Hive Support
    """    

    config = configparser.ConfigParser()
    config.read('aws_credentials.cfg')
    access_id=config.get('AWS', 'AWS_ACCESS_KEY_ID')
    access_key=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
     
    spark = SparkSession \
        .builder \
        .appName("CapstoneProject")\
        .config("spark.sql.debug.maxToStringFields","30")\
        .config("spark.jars.packages","saurfang:spark-sas7bdat:3.0.0-s_2.12,org.apache.hadoop:hadoop-aws:2.7.3")\
        .config("spark.sql.legacy.createHiveTableByDefault","false")\
        .config("spark.hadoop.fs.s3a.access.key", access_id)\
        .config("spark.hadoop.fs.s3a.secret.key",access_key)\
        .enableHiveSupport().getOrCreate()

    sc=spark.sparkContext
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.block.size","32000000")
    hadoop_conf.set("fs.s3a.multipart.size","104857600")
    hadoop_conf.set("fs.s3a.threads.core","4")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    return spark

def process_sas_data(spark, input_data):
    """
     Reads parquet files for sas_data from the s3 bucket bucket-capstone.
     Executes the drop_tables_queries and create_table_queries imported from the sql_queries
    """
    sas_data=os.path.join(input_data, 'sas_data','*.parquet')
    
    # read sas_data file
    #df_spark=spark.read.load('sas_data')
    df_spark=spark.read.parquet('sas_data')
    #create a temporary view to use in SQL statements
    df_spark.createOrReplaceTempView("sas_data2")

    for query in drop_table_queries:
        spark.sql(query)

    for query in create_table_queries:
        spark.sql(query)
       
def main():
    """
    defines the url for s3 bucket being read by the process_sas_data,
    sets up the environment to create spark session and calls the functions
    """
    
    #setting up environment to read sas_data folder
    os.environ["JAVA_HOME"] = "/Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home"
    os.environ["PATH"] = "/Users/sudhanagrath/anaconda3/bin:/Users/sudhanagrath/anaconda3/condabin:/Users/sudhanagrath/spark-3.1.2-bin-hadoop2.7/bin:/Users/sudhanagrath/scala-2.13.6/bin:/Library/Frameworks/Python.framework/Versions/3.8/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/Applications/Postgres.app/Contents/Versions/latest/bin"
    os.environ["SPARK_HOME"] = "/Users/sudhanagrath/spark-3.1.2-bin-hadoop2.7"
   
    spark = create_spark_session()

    input_data = "s3a://bucket-capstone/"
    process_sas_data(spark, input_data)    
    
if __name__ == "__main__":
    main()
