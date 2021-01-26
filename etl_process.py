import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from utility import *

"""
In below code we are reading dl.cfg file
for aws credetials
"""
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

"""
In below part of code we are creating spark session and setting log level
"""

def create_spark_session():
    spark = SparkSession \
           .builder\
           .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
           .enableHiveSupport()\
           .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    return spark

"""
In below part of code we have written function to process
us-cities-demographics.csv file to load lu_usa_demographics
table
"""

def processing_demographics_file(spark,input_data,output_data,demographics_file_nm):
    demographics_file = input_data+demographics_file_nm
    df = spark.read.format("csv").option("header", "true").option("delimiter",";").load(demographics_file)
    cleaned_df = cleaning_demographics_file(df)
    demographics_table = cleaned_df.select(
    col("City").alias("city"),
    col("State").alias("state"),
    col("State Code").alias("state_cd"),
    col("Race").alias("race"),
    col("Median Age").alias("median_age"),
    col("Male Population").alias("male_population"),
    col("Female Population").alias("female_population"),
    col("Total Population").alias("total_population"),
    col("Number of Veterans").alias("number_of_veterans"),
    col("Foreign-born").alias("foreign_born"),
    col("Average Household Size").alias("avg_house_hold_size"),
    col("Count").alias("count")
    )

    demographics_table = demographics_table.withColumn("id",monotonically_increasing_id())

    demographics_table_name = "lu_usa_demographics"

    data_vlidation(spark,demographics_table_name,demographics_table)

    demographics_table.write.mode('overwrite').parquet("{}/parquet/lu_usa_demographics.parquet".format(output_data))

    print("Data loaded successfully in lu_usa_demographics table.")

"""
In below part of code we have written function to process
GlobalLandTemperaturesByCity.csv file to load lu_country
table
"""

def processing_temprature_res_mapping(spark,input_data,output_data,temprature_file_nm,res_mapping_file_nm):
    temprature_file_path = input_data+temprature_file_nm
    res_mapping_file_path = input_data+res_mapping_file_nm

    temp_df = spark.read.csv(temprature_file_path,header = True)
    cleaned_temp_df = cleaning_temprature_file(temp_df)
    temp_df.createOrReplaceTempView("temp_view")
    temp_df = spark.sql("""
                        select Country,
                        avg(AverageTemperature) as avg_temp
                        from temp_view
                        group by Country
                        """)

    res_df = spark.read.csv(res_mapping_file_path,header = True)

    temp_df.createOrReplaceTempView("temp_view")
    res_df.createOrReplaceTempView("res_view")
    final_df = spark.sql("""select
                            res_view.code as country_cd,
                            res_view.Name as country_nm,
                            round(temp_view.avg_temp,2) as avg_temprature
                            from res_view
                            left join temp_view
                            on lower(res_view.Name) = lower(temp_view.Country) """)

    country_table_name = "lu_country"

    data_vlidation(spark,country_table_name,final_df)

    final_df.write.mode('overwrite').parquet("{}/parquet/lu_country.parquet".format(output_data))
    print("Data loaded successfully in lu_country table.")


"""
In below part of code we have written function to process
i94mode.csv file to load lu_travel_mode
table
"""

def processing_mode_mapping_file(spark,input_data,output_data,mode_mapping_file_nm):
    mapping_file_path = input_data+mode_mapping_file_nm
    mode_df = spark.read.csv(mapping_file_path, header = True)
    mode_df.write.mode('overwrite').parquet("{}/parquet/lu_travel_mode.parquet".format(output_data))
    mode_table_name = "lu_travel_mode"
    data_vlidation(spark,mode_table_name,mode_df)
    print("Data loaded successfully in lu_travel_mode table.")

"""
In below part of code we have written function to process
airport-codes_csv.csv file to load lu_port
table
"""

def processing_airport_file(spark,input_data,output_data,airport_file_nm):
    airport_file_path = input_data+airport_file_nm
    airport_df = spark.read.csv(airport_file_path,header = True)
    cleaned_airport_df = cleaning_airport_file(airport_df)
    cleaned_airport_df = cleaned_airport_df.select(
    col("iata_code").alias("iata_cd"),
    col("type").alias("port_type"),
    col("name").alias("port_name"),
    col("elevation_ft").alias("elevation"),
    col("continent").alias("continet"),
    col("iso_country").alias("iso_country"),
    col("iso_region").alias("iso_region"),
    col("municipality").alias("municipality"),
    col("coordinates").alias("coordinates")
    )
    airport_table_name = "lu_port"
    data_vlidation(spark,airport_table_name,cleaned_airport_df)
    cleaned_airport_df.write.mode('overwrite').parquet("{}/parquet/lu_port.parquet".format(output_data))
    print("Data loaded successfully in lu_port table.")


"""
In below part of code we have written function to process
USA immigration data to load lu_arrival_calendar,
lu_visa and fact_usa_immigration tables.
"""

def processing_immigration_file(spark,input_data,output_data,immigration_file_nm):
    immigration_file_path = input_data+immigration_file_nm
    immigration_df = spark.read.parquet(immigration_file_path)

    arr_dt_df = cleaning_immigration_file_arr_dt_prep(immigration_df)
    arr_dt_df = arr_dt_df.select(
    col("arrdate").alias("arr_dt_cd"),
    col("arr_date").alias("arr_date"),
    dayofmonth("arr_date").alias("arr_day"),
    month("arr_date").alias("arr_month"),
    year("arr_date").alias("arr_year"),
    weekofyear("arr_date").alias("arr_week"),
    date_format("arr_date", "EEEE").alias("arr_weekday")
    )
    date_table_name = "lu_arrival_calendar"
    data_vlidation(spark,date_table_name,arr_dt_df)
    arr_dt_df.write.partitionBy("arr_year","arr_month").mode('overwrite').parquet("{}/parquet/lu_arrival_calendar.parquet".format(output_data))
    print("Data loaded successfully in lu_arrival_calendar table.")

    visa_df = cleaning_immigration_file_visa_prep(immigration_df)
    visa_df = visa_df.withColumn("id",monotonically_increasing_id())
    visa_df = visa_df.select(
    col("id").alias("visa_type_id"),
    col("visatype").alias("visa_type")
    )
    visa_table_name = "lu_visa"
    data_vlidation(spark,visa_table_name,visa_df)
    visa_df.write.mode('overwrite').parquet("{}/parquet/lu_visa.parquet".format(output_data))
    print("Data loaded successfully in lu_visa table.")

    fact_df = cleaning_immigration_file_fact_prep(immigration_df)
    fact_df.createOrReplaceTempView("fact_view")
    visa_df.createOrReplaceTempView("visa_view")
    arr_dt_df.createOrReplaceTempView("calendar_view")
    final_df = spark.sql("""
                           select
                           fact_view.cicid as cic_id,
                           visa_view.visa_type_id,
                           fact_view.i94res as country_cd,
                           fact_view.i94port as port_cd,
                           fact_view.arrdate as arr_dt_cd,
                           fact_view.i94mode as mode_id,
                           fact_view.i94addr as state_cd,
                           fact_view.depdate as depdate,
                           fact_view.i94bir as age_of_respondent,
                           fact_view.count as count,
                           fact_view.biryear as birth_year,
                           fact_view.gender as gender,
                           fact_view.airline as airline,
                           fact_view.fltno as fltno
                           from fact_view
                           left join visa_view
                           on fact_view.visatype = visa_view.visa_type
                         """)
    get_timestamp = udf(lambda x: (datetime.datetime(1960, 1, 1).date() + datetime.timedelta(x)).isoformat() if x else None)
    final_df = final_df.withColumn("arr_date", get_timestamp(final_df.arr_dt_cd))
    final_df = final_df.withColumn("arr_month",month("arr_date"))
    final_df = final_df.withColumn("arr_year",year("arr_date"))
    fact_table_name = "fact_usa_immigration"
    data_vlidation(spark,fact_table_name,final_df)
    final_df.write.partitionBy("arr_year","arr_month").mode('append').parquet("{}/parquet/fact_usa_immigration.parquet".format(output_data))
    print("Data loaded successfully in fact_usa_immigration table.")

"""
Below is the main function where are setting variables for
input path, output path anf for all source file names. Also,
We are executing all the funcetions defind above to process
all source files and load target tables.
"""
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-capstone-project-8796/input-folder/"
    output_data = "s3a://udacity-capstone-project-8796/output-folder/"

    immigration_file_nm = "sas_data/part-00000-b9542815-7a8d-45fc-9c67-c9c5007ad0d4-c000.snappy.parquet"
    airport_file_nm = "airport-codes_csv.csv"
    temprature_file_nm = "GlobalLandTemperaturesByCity.csv"
    mode_mapping_file_nm = "i94mode.csv"
    res_mapping_file_nm = "i94res.csv"
    demographics_file_nm = "us-cities-demographics.csv"

    processing_demographics_file(spark,input_data,output_data,demographics_file_nm)

    processing_temprature_res_mapping(spark,input_data,output_data,temprature_file_nm,res_mapping_file_nm)

    processing_mode_mapping_file(spark,input_data,output_data,mode_mapping_file_nm)

    processing_airport_file(spark,input_data,output_data,airport_file_nm)

    processing_immigration_file(spark,input_data,output_data,immigration_file_nm)

if __name__ == "__main__":
    main()
