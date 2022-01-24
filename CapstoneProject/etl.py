import configparser
import os
from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as f
import pyspark.sql.types as t
from datetime import timedelta
from sql_queries import insert_table_queries, insert_table_csv_queries, export_us_cities_schema,export_us_state_code_schema,export_port_city_state_schema,export_country_lookup_schema

#defining function to use to populate the i94_time table
def date_add(date,days):
    if type(date) is not datetime:
        date=datetime.strptime('1960-01-01','%Y-%m-%d')
    return date + timedelta(days)

#adding the function to function module of Spark Functions Module
date_add_udf=f.udf(date_add, t.DateType())



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
        .config("spark.sql.legacy.createHiveTableByDefault","false")\
        .config("spark.jars.packages","saurfang:spark-sas7bdat:3.0.0-s_2.12,org.apache.hadoop:hadoop-aws:2.7.3")\
        .config("spark.hadoop.fs.s3a.access.key", access_id)\
        .config("spark.hadoop.fs.s3a.secret.key",access_key)\
        .enableHiveSupport().getOrCreate()

   #Configuring Hadoop properties through Spark Context to resolve runtime errors
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
     Execute the insert_table_queries defined in the sql_queries module
    """

    # read sas_data file
    #df_spark=spark.read.load('sas_data')
    df_spark=spark.read.parquet('sas_data')
    #create a temporary view to use in SQL statements
    df_spark.createOrReplaceTempView("sas_data2")

    #Registering the function to Spark Session
    spark.udf.register("date_add_udf",date_add_udf)
 
    for query in insert_table_queries:
        spark.sql(query)

   
def process_countries_data(spark):
    """
     This function reads the CSV files from s3 bucket and their schemas to create data frames
     Executes the insert_table_csv_queries defined in the sql_queries
    """
    df_us_cities=spark.read\
                      .option("header", "true")\
                      .option("delimiter", ";")\
                      .schema(export_us_cities_schema)\
                      .csv('s3a://bucket-capstone/countries_data/us-cities-demograhics.csv')
    df_us_cities.createOrReplaceTempView("us_states_cities")

    df_us_state_code=spark.read.option("header", "true")\
                               .option("delimiter", "\t")\
                               .schema(export_us_state_code_schema)\
                               .csv('s3a://bucket-capstone/countries_data/us-state-codes.csv')
    df_us_state_code.createOrReplaceTempView("us_state_code")

    df_port_city_state=spark.read.option("header", "true")\
                                 .option("delimiter",";")\
                                 .schema(export_port_city_state_schema)\
                                 .csv('s3a://bucket-capstone/countries_data/port-city-state.csv')
    df_port_city_state.createOrReplaceTempView("port_city_state")

    df_country_lookup=spark.read.option("header", "true")\
                                .option("delimiter", "\t")\
                                .schema(export_country_lookup_schema)\
                                .csv('s3a://bucket-capstone/countries_data/country_lookup.csv')
    df_country_lookup.createOrReplaceTempView("country_lookup")

    for query in insert_table_csv_queries:
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
        process_countries_data(spark)

        
        


if __name__ == "__main__":
        main()
