import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,length
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import SQLContext

"""
In below part of code we have written function to clean
data of us-cities-demographics.csv file
"""

def cleaning_demographics_file(df):
    subset_columns = ["Median Age",
                      "Male Population",
                      "Female Population",
                      "Total Population",
                      "Number of Veterans",
                      "Foreign-born",
                      "Average Household Size",
                      "Count"]
    record_count_before = df.count()
    df.dropna(subset = subset_columns,how = "all")
    df.drop_duplicates(subset = ["City","State","State Code","Race"])
    print("Number of records dropped after demographics file cleaning are {}".format(record_count_before - df.count()))
    return df

"""
In below part of code we have written function to clean
data of GlobalLandTemperaturesByCity.csv file
"""
def cleaning_temprature_file(df):
    record_count_before = df.count()
    df = df.filter("AverageTemperature is not null")
    df = df.drop_duplicates(subset = ["dt","City","Country"])
    print("Number of records dropped after temprature file cleaning are {}".format(record_count_before - df.count()))
    return df

"""
In below part of code we have written function to clean
data of airport-codes_csv.csv file
"""

def cleaning_airport_file(df):
    record_count_before = df.count()
    df = df.withColumn('length_column', length('iata_code'))
    df = df.filter(col('length_column') == 3)
    df = df.select(['iata_code','type','name','elevation_ft','continent','iso_country','iso_region','municipality','coordinates'])
    df = df.drop_duplicates(subset = ["iata_code"])
    print("Number of records dropped after airport file cleaning are {}".format(record_count_before - df.count()))
    return df

"""
In below part of code we have written function to clean
data of USA immigration for lu_arrival_calendar
"""

def cleaning_immigration_file_arr_dt_prep(df):
    record_count_before = df.count()
    df = df.drop_duplicates(subset = ["arrdate"])
    df = df.dropna(subset = ["arrdate"],how = "all")
    df = df.select(["arrdate"])
    df = df.distinct()
    get_timestamp = udf(lambda x: (datetime.datetime(1960, 1, 1).date() + datetime.timedelta(x)).isoformat() if x else None)
    df = df.select(['arrdate']).withColumn("arr_date", get_timestamp(df.arrdate))
    print("Number of records dropped after arrival date dataset cleaning are {}".format(record_count_before - df.count()))
    return df

"""
In below part of code we have written function to clean
data of USA immigration for lu_visa
"""

def cleaning_immigration_file_visa_prep(df):
    record_count_before = df.count()
    df = df.drop_duplicates(subset = ["visatype"])
    df = df.dropna(subset = ["visatype"],how = "all")
    df = df.select(["visatype"])
    df = df.distinct()
    print("Number of records dropped after visa type dataset cleaning are {}".format(record_count_before - df.count()))
    return df

"""
In below part of code we have written function to clean
data of USA immigration for fact_usa_immigration
"""

def cleaning_immigration_file_fact_prep(df):
    record_count_before = df.count()
    df = df.drop_duplicates(subset = ["cicid"])
    df = df.dropna(how = "all")
    df = df.select(["cicid","visatype","i94res","i94port","arrdate",
                    "i94mode","i94addr","depdate","i94bir","count",
                    "biryear","gender","airline","fltno"])
    df = df.filter("""cicid is not null and
                      visatype is not null and
                      i94res is not null and
                      i94port is not null and
                      arrdate is not null and
                      i94mode is not null and
                      i94addr is not null and
                      depdate is not null and
                      i94bir is not null and
                      count is not null and
                      biryear is not null and
                      gender is not null and
                      airline is not null and
                      fltno is not null""")
    print("Number of records dropped after fact table dataset cleaning are {}".format(record_count_before - df.count()))
    return df

"""
With the help of below function we are validating data.
"""

def data_vlidation(spark,table_name,df):
    record_count = df.count()
    if record_count != 0 :
        print("Table data load of {} is data validation Step for count is passed with number of records {}.".format(table_name,record_count))
    else:
        print("Table data load of {} is data validation Step for count is failed number with of records {}.".format(table_name,record_count))

    query_dict = {"fact_usa_immigration":"select cic_id,count(*) from temp_view group by cic_id having count(*)>1",
                 "lu_usa_demographics":"select id,count(*) from temp_view group by id having count(*)>1",
                 "lu_arrival_calendar":"select arr_dt_cd,count(*) from temp_view group by arr_dt_cd having count(*)>1",
                 "lu_country":"select country_cd,count(*) from temp_view group by country_cd having count(*)>1",
                 "lu_visa":"select visa_type_id,count(*) from temp_view group by visa_type_id having count(*)>1",
                 "lu_port":"select iata_cd,count(*) from temp_view group by iata_cd having count(*)>1",
                 "lu_travel_mode":"select mode_id,count(*) from temp_view group by mode_id having count(*)>1"}

    sql_query = query_dict[table_name]
    df.createOrReplaceTempView("temp_view")
    pk_df = spark.sql(sql_query)
    df_count = pk_df.count()
    if df_count == 0:
        print("Primary Key duplication validation step for table {} is passed.".format(table_name))
    else:
        print("Primary Key duplication validation step for table {} is failed.".format(table_name))
    return
