import configparser
from pyspark.sql.types import StructType, DoubleType, StringType,StructField

#Drop table queries for hive internal tables, two fact tables and 9 dimension tables 
export_i94_table_drop="DROP TABLE IF EXISTS I94_TABLE_EXPORT;"
export_i94_visitor_drop="DROP TABLE IF EXISTS I94_VISITOR_EXPORT;"
export_visa_table_drop="DROP TABLE IF EXISTS I94_VISA_EXPORT;"
export_arr_table_drop="DROP TABLE IF EXISTS I94_ARR_STATUS_EXPORT;"
export_dep_table_drop="DROP TABLE IF EXISTS I94_DEP_STATUS_EXPORT;"                      
export_travel_table_drop="DROP TABLE IF EXISTS I94_TRAVEL_MODE_EXPORT;"
export_time_table_drop="DROP TABLE IF EXISTS I94_TIME_EXPORT;"
export_us_cities_drop="DROP TABLE IF EXISTS US_CITIES_EXPORT;"
export_us_state_code_drop="DROP TABLE IF EXISTS US_STATE_CODE_EXPORT;"
export_port_city_state_drop="DROP TABLE IF EXISTS PORT_CITY_STATE_EXPORT;"
export_country_lookup_drop="DROP TABLE IF EXISTS COUNTRY_LOOKUP_EXPORT;"

#Drop table queries for hive external tables, two tact tables and 9 dimension tables 
import_i94_table_drop="DROP TABLE IF EXISTS I94_TABLE;"
import_i94_visitor_drop="DROP TABLE IF EXISTS I94_VISITOR;"
import_visa_table_drop="DROP TABLE IF EXISTS I94_VISA;"    
import_arr_table_drop="DROP TABLE IF EXISTS I94_ARR_STATUS;"    
import_dep_table_drop="DROP TABLE IF EXISTS I94_DEP_STATUS;"   
import_travel_table_drop="DROP TABLE IF EXISTS I94_TRAVEL_MODE;"    
import_time_table_drop="DROP TABLE IF EXISTS I94_TIME;"
import_us_cities_drop="DROP TABLE IF EXISTS US_CITIES;"
import_us_state_code_drop="DROP TABLE IF EXISTS US_STATE_CODE;"
import_port_city_state_drop="DROP TABLE IF EXISTS PORT_CITY_STATE2;"
import_country_lookup_drop="DROP TABLE IF EXISTS COUNTRY_LOOKUP;"

#Create tables for seven Parquet files to load into Hive embedded Derby Database
export_i94_table_create="""CREATE TABLE I94_TABLE_EXPORT(
cicid double,i94yr double,i94mon double,i94cit double,i94res double,i94port string,arrdate double,i94mode double,
i94addr string,depdate double,i94bir double,i94visa double, entdepa string,entdepd string,
entdepu string,biryear double,gender string,admnum double,visatype string);"""
export_i94_visitor_create="""CREATE TABLE I94_VISITOR_EXPORT(
       admnum double,
       i94bir double,
       biryear double,
       gender CHAR(1),
       i94addr CHAR(2),
       occup VARCHAR(30),
       visapost CHAR(3));"""

export_visa_table_create="CREATE TABLE I94_VISA_EXPORT(visa_code double, visa_description VARCHAR(20));"
export_arr_table_create="CREATE TABLE I94_ARR_STATUS_EXPORT(arr_code CHAR(1), description VARCHAR(100));"
export_dep_table_create="CREATE TABLE I94_DEP_STATUS_EXPORT(dep_code CHAR(1), description VARCHAR(100));"                      
export_travel_table_create="CREATE TABLE I94_TRAVEL_MODE_EXPORT(travel_mode double, travel_description VARCHAR(20));"
export_time_table_create="""CREATE TABLE I94_TIME_EXPORT(sas_date double ,cal_date date, year int, month int, week int, day int);"""

#Create schemas for four CSV files to load into Hive embedded Derby database
export_us_cities_schema=StructType([
                         StructField("City",StringType(),True),\
                         StructField("State",StringType(),True),\
                         StructField("Median_Age",DoubleType(),True),\
                         StructField("Male_Population",DoubleType(),True),\
                         StructField("Female_Population",DoubleType(),True),\
                         StructField("Total_Population",DoubleType(),True),\
                         StructField("Number_of_Veterans",DoubleType(),True),\
                         StructField("Foreign_Born",DoubleType(),True),\
                         StructField("Average_Household_Size",DoubleType(),True),\
                         StructField("State_Code", StringType()),\
                         StructField("Race",StringType()),\
                         StructField("Count",DoubleType(),True)])
export_us_state_code_schema=StructType([
					StructField("CODE",StringType(),True),\
					StructField("STATE",StringType(),True)])
export_port_city_state_schema=StructType([
                                          StructField("CODE",StringType(),True),\
                                          StructField("CITY_STATE",StringType(),True)])
export_country_lookup_schema=StructType([
                                         StructField("CODE",StringType(),True),\
                                         StructField("COUNTRY_CITY",StringType(),True)])

#Create Hive External Tables for 7 Parquet files     
import_i94_table_create="""CREATE TABLE I94_TABLE STORED AS PARQUET LOCATION 's3a://bucket-hive/i94-table'
                         AS SELECT * FROM I94_TABLE_EXPORT;"""
import_i94_visitor_create="""CREATE TABLE I94_VISITOR(
       admnum double,
       i94bir double,
       biryear double,
       gender CHAR(1),
       i94addr CHAR(2),
       occup VARCHAR(30),
       visapost CHAR(3))
       STORED AS PARQUET LOCATION 's3a://bucket-hive/i94-visitor';"""
import_visa_table_create="""CREATE TABLE I94_VISA(visa_code double, visa_description VARCHAR(20))
                            STORED AS PARQUET LOCATION 's3a://bucket-hive/i94-visa';"""   
import_arr_table_create="""CREATE TABLE I94_ARR_STATUS(arr_code CHAR(1), description VARCHAR(100))
                           STORED AS PARQUET LOCATION 's3a://bucket-hive/i94-arr-status';""" 
import_dep_table_create="""CREATE TABLE I94_DEP_STATUS(dep_code CHAR(1), description VARCHAR(100))
                           STORED AS PARQUET LOCATION 's3a://bucket-hive/i94-dep-status';"""
import_travel_table_create="""CREATE TABLE I94_TRAVEL_MODE(travel_mode double, travel_description VARCHAR(20))
                              STORED AS PARQUET LOCATION 's3a://bucket-hive/i94-travel-mode';"""
import_time_table_create="""CREATE TABLE I94_TIME(sas_date double,cal_date date, year int, month int, week int, day int)
                            STORED AS PARQUET LOCATION 's3a://bucket-hive/i94-time';"""
                            
#Insert data into Hive Internal Tables from Parquet files
export_i94_table_insert="""INSERT OVERWRITE I94_TABLE_EXPORT
                        SELECT cicid,i94yr,i94mon,i94cit,i94res,i94port,arrdate,i94mode,i94addr,depdate,i94bir,
                        i94visa,entdepa,entdepd,entdepu,biryear,gender,admnum,visatype
                        from sas_data2;"""
export_i94_visitor_insert="""INSERT OVERWRITE I94_VISITOR_EXPORT SELECT admnum, i94bir, biryear, gender, i94addr, occup, visapost from sas_data2;"""
export_visa_table_insert="""INSERT OVERWRITE I94_VISA_EXPORT SELECT distinct i94visa,
                            case when i94visa=1.0 then 'Business' when i94visa=2.0 then 'Pleasure'
                            else 'Student' end as visa from sas_data2 where i94visa is not null;"""
export_arr_table_insert="""INSERT OVERWRITE I94_ARR_STATUS_EXPORT SELECT distinct entdepa,
                           case when entdepa='K' then 'Lost i94 or is deceased' when entdepa='T' then 'Overstayed' when entdepa='O' then 'Paroled into US'
                           when entdepa='Z' then 'Adjusted to Permanent Residence' else null end as description from sas_data2 where entdepa is not null;"""
export_dep_table_insert="""INSERT OVERWRITE I94_DEP_STATUS_EXPORT SELECT distinct entdepd,
                           case when entdepd='K' then 'Lost i94 or is deceased' when entdepd='O' then 'Paroled into US'  when entdepd='N' then 'Apprehended'
                           when entdepd='R' then 'Departed' when entdepd='M' then 'Matches' else null end as description from sas_data2 where entdepd is not null;"""
          
export_travel_table_insert="""INSERT OVERWRITE I94_TRAVEL_MODE_EXPORT SELECT distinct i94mode,
                              case when i94mode=1.0 then 'Air' when i94mode=2.0 then 'Sea' when i94mode=3.0 then 'Land' else 'Not Reported' end as travel_description
                              from sas_data2 where i94mode is not null;"""
export_time_table_insert="""INSERT OVERWRITE I94_TIME_EXPORT SELECT sn.sas_date, date_add_udf('1960-01-01',sn.sas_date) as cal_date,
                            extract(year from date_add_udf('1960-01-01',sn.sas_date)) as year,
                            extract(month from date_add_udf('1960-01-01',sn.sas_date)) as month,
                            extract(week from date_add_udf('1960-01-01',sn.sas_date)) as week,
                            extract(day from date_add_udf('1960-01-01',sn.sas_date)) as day
                            from ((select distinct arrdate as sas_date from sas_data2 where arrdate is not null) union all
                            (select distinct depdate as sas_date from sas_data2 where depdate is not null))sn;"""

#Create and load Hive Internal Tables from CSV files
export_us_cities_insert="CREATE TABLE US_CITIES_EXPORT AS SELECT * FROM us_states_cities"
export_us_state_code_insert="CREATE TABLE US_STATE_CODE_EXPORT AS SELECT * FROM US_STATE_CODE"
export_port_city_state_insert="CREATE TABLE PORT_CITY_STATE_EXPORT AS SELECT * FROM PORT_CITY_STATE"
export_country_lookup_insert="CREATE TABLE COUNTRY_LOOKUP_EXPORT AS SELECT * FROM COUNTRY_LOOKUP"

#Insert data into Hive External Tables from the Hive Internal Tables
import_i94_table_insert="""INSERT OVERWRITE I94_TABLE SELECT * FROM I94_TABLE_EXPORT;"""
import_i94_visitor_insert="""INSERT OVERWRITE I94_VISITOR SELECT * FROM I94_VISITOR_EXPORT;"""
import_visa_table_insert="""INSERT OVERWRITE I94_VISA SELECT * FROM I94_VISA_EXPORT;"""   
import_arr_table_insert="""INSERT OVERWRITE I94_ARR_STATUS SELECT * from I94_ARR_STATUS_EXPORT;"""    
import_dep_table_insert="""INSERT OVERWRITE I94_DEP_STATUS  SELECT * from I94_DEP_STATUS_EXPORT;"""   
import_travel_table_insert="""INSERT OVERWRITE I94_TRAVEL_MODE SELECT * from I94_TRAVEL_MODE_EXPORT;"""
import_time_table_insert="""INSERT OVERWRITE I94_TIME SELECT * from I94_TIME_EXPORT;"""

#Create and load Hive External Tables from the Hive Internal Tables
import_us_cities_insert="CREATE TABLE US_CITIES LOCATION 's3a://bucket-hive/us_cities' AS SELECT * FROM US_CITIES_EXPORT;"
import_us_state_code_insert="CREATE TABLE US_STATE_CODE LOCATION 's3a://bucket-hive/us_state_codes' AS SELECT * FROM US_STATE_CODE_EXPORT;"
import_port_city_state_insert="CREATE TABLE PORT_CITY_STATE2 LOCATION 's3a://bucket-hive/port_city_state' AS SELECT * FROM PORT_CITY_STATE_EXPORT;"
import_country_lookup_insert="CREATE TABLE COUNTRY_LOOKUP LOCATION 's3a://bucket-hive/country_lookup' AS SELECT * FROM COUNTRY_LOOKUP_EXPORT;"

#table count for quality_checks
i94_table_count="select count(*) from i94_table_export;"
i94_visitor_count="select count(*) from i94_visitor;"
i94_visa_count="select count(*) from i94_visa;"
i94_arr_status_count="select count(*) from i94_arr_status;"
i94_dep_status_count="select count(*) from i94_dep_status;"
i94_travel_mode_count="select count(*) from i94_travel_mode;"
i94_time_count="select count(*) from i94_time;"
us_cities_count="select count(*) from us_cities_export;"
us_state_code_count="select count(*) from us_state_code;"
port_city_state_count="select count(*) from port_city_state_export;"
country_lookup_count="select count(*) from country_lookup;"

#key count for quality_checks
i94_table_key="select count(cicid) as key_count from i94_table_export;"
i94_visitor_key="select count(admnum) as key_count from i94_visitor_export;"
i94_visa_key="select count(visa_code) as key_count from i94_visa;"
i94_arr_status_key="select count(arr_code) as key_count from i94_arr_status;"
i94_dep_status_key="select count(dep_code) as key_count from i94_dep_status;"
i94_travel_mode_key="select count(travel_mode) as key_count from i94_travel_mode;"
i94_time_key="select count(sas_date) as key_count from i94_time;"
us_cities_key="select count(city) as key_count from us_cities_export;"
us_state_code_key="select count(code) as key_count from us_state_code;"
port_city_state_key="select count(code) as key_count from port_city_state_export;"
country_lookup_key="select count(code) as key_count from country_lookup;"
              
             

#List of Queries
drop_table_queries=[export_i94_table_drop,export_i94_visitor_drop, export_visa_table_drop,export_arr_table_drop, export_dep_table_drop,\
                    export_travel_table_drop, export_time_table_drop,import_i94_table_drop, import_i94_visitor_drop,import_visa_table_drop,\
                    import_arr_table_drop,import_dep_table_drop, import_travel_table_drop,import_time_table_drop,\
                    export_us_cities_drop, export_us_state_code_drop, export_port_city_state_drop, export_country_lookup_drop,\
                    import_us_cities_drop, import_us_state_code_drop, import_port_city_state_drop, import_country_lookup_drop]
                    
create_table_queries=[export_i94_table_create,export_i94_visitor_create,export_visa_table_create,export_arr_table_create,\
                      export_dep_table_create,export_travel_table_create, export_time_table_create,\
                      import_i94_table_create,import_i94_visitor_create,import_visa_table_create,\
                      import_arr_table_create, import_dep_table_create,import_travel_table_create,import_time_table_create]                      
insert_table_queries=[export_i94_table_insert,export_i94_visitor_insert, export_visa_table_insert,export_arr_table_insert, export_dep_table_insert,\
                      export_travel_table_insert,export_time_table_insert,\
                      import_i94_table_insert,import_i94_visitor_insert,import_visa_table_insert,\
                      import_arr_table_insert,import_dep_table_insert, import_travel_table_insert,import_time_table_insert]
                      
insert_table_csv_queries=[export_us_cities_insert,export_us_state_code_insert,export_port_city_state_insert,export_country_lookup_insert,\
                      import_us_cities_insert,import_us_state_code_insert,import_port_city_state_insert,import_country_lookup_insert]

table_counts=[i94_table_count, i94_visitor_count, i94_visa_count, i94_arr_status_count, i94_dep_status_count,i94_travel_mode_count,i94_time_count,\
              us_cities_count, us_state_code_count,port_city_state_count,country_lookup_count]
              
key_counts=[i94_table_key, i94_visitor_key, i94_visa_key, i94_arr_status_key, i94_dep_status_key,i94_travel_mode_key,i94_time_key,\
              us_cities_key, us_state_code_key,port_city_state_key,country_lookup_key]
              

    
   
    
    
    
    
   
    
    
    
    
    
    

 

   
