# Data Engineering Capstone Project
## US Immigration Database
### Scope of Project:
In this project we are using USA immigration data in order to explore data behaviour of passenger. Also in this project we are using data which is collected from different source which are different in nature for example, airport data, tempraure data etc. to support analysis and make this analysis more insigntful. In this project we are using aws S3 to store input and output data also aws EMR cluster to porcess and convert input data into required format with the help of spark.

### Source Datasets:
In this project we are using below datasets which are extracted from different sources and then loaded them into S3 bucket.
#### 1. I94 Immigration Data:
This data is taken from the [US National Tourism and Trade Office](https://travel.trade.gov/research/reports/i94/historical/2016.html). This includes visitor data who are arrived in USA (I-94 records) and below are the list of columns.
Column Name | Specification
--- | --- 
cicid | Unique record Id
i94yr | 4 digit year
i94mon | Numeric month
i94cit and i94res | Country Code
i94port | Port Code
arrdate |  Visitor arrival Date in the USA
i94mode | Code for mode of travel
i94addr | State Code
depdate | Visitor departure Date from the USA
i94bir | Age of Respondent in Years
i94visa | Visa codes
count | count
dtadfile | Date added to I-94 Files
visapost | Department of State where where Visa was issued
occup |  Occupation that will be performed in U.S
entdepa | Arrival Flag - admitted or paroled into the U.S.
entdepd | Departure Flag - Departed, lost I-94 or is deceased
entdepu | Update Flag - Either apprehended, overstayed, adjusted to perm residence
matflag | Match flag - Match of arrival and departure records
biryear | 4 digit year of birth
dtaddto | Date to which admitted to U.S.
gender | Non-immigrant sex
insnum | INS number
airline | Airline used to arrive in U.S.
admnum | Admission Number
fltno | Flight number of Airline used to arrive in U.S.
visatype | Class of admission legally admitting the non-immigrant to temporarily stay in U.S.

#### 2. World Temperature Data:
In this dataset we are having temprature data of city and it is taken for [kaggle](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data). This dataset contain below columns.
Column Name | Specification
--- | --- 
dt | Date for which temprature is recorded
AverageTemperature | Average temprature of city.
AverageTemperatureUncertainty | Average temprature uncerntainity of city.
City | City for which temprature is recorded
Country | Country of city
Latitude | Lattitude of city
Longitude | Longitude of city

#### 3. U.S. City Demographic Data:
This dataset is taken from  OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). In this dataset we have data related to population, Foreign-born and other details given below.
Column Name | Specification
--- | --- 
City | City for which details are given
State | State name of city
Median Age | Median age of people living in city
Male Population | Male population in the city
Female Population | Female Population in the city
Total Population | Total Population of the city
Number of Veterans | Number of Veterans in the city
Foreign-born | Number of people born in foreign
Average Household Size | Average house hold size of city
State Code | State Code of city
Race | Race of people
Count | count

#### 4. Airport Code Table:
This is a simple table of airport codes and corresponding cities. It comes from [here](https://datahub.io/core/airport-codes#data). This dataset contain below columns.

Column Name | Specification
--- | --- 
ident | Id Column
type | Type of port
name | Name of port
elevation_ft | elevation of port
continent | continent of port
iso_country | Country of port
iso_region | Region of port
municipality | Municipality of port
gps_code | gps code of port
iata_code | code of port
local_code | local code of port
coordinates | coordinates of port

### Prerequisites:
All source data is loaded in S3 bucket and aws EMR cluster is used to process data.
* AWS S3 Bucket
* AWS EMR Cluster for Spark processing
* configparser python 3 is needed to run the python scripts.

### Code:
In this project added below script files to process files.
* etl_process.py : This File contain script to read file from S3 and process then using spark and after processing load them back to S3 bucket in required parquet folder.
* utility.py : This file contain module in which we have defined functions to validate, clean and remove unrequired columns from dataset.
* dl.cfg : In this file we have defined all configurations required to run above scripts.
* Capstone Project Template.ipynb :  In this we have given steps taken to explore and validate source datasets.

### Steps Taken to Complete this Project.
* Step 1 : Scope the Project and Gather Data
* Step 2 : Explore and Assess the Data
* Step 3 : Define the Data Model
* Stpe 4 : Run ETL to Model the Data

#### Step 1 : Scope the Project and Gather Data
In this project we are using datasets explained above and using these datasets we are developing analytics database to analyze behaviour of visitors, aiport etc. Below is the plan to develop databse.
* First we will be reading immigration dataset and try to identify nature of columns (which columns required and blank values in the columns). We will explore data of this dataset and we will create three tables from this dataset.
  * Fact Table : This table will contian all foreign keys and factual data required to do analysis.
  * Visa Dimension Table : We will take all distinct types of visa from this dataset and load them into dimnesion table with unique id. This table can join with fact using unique id
  * Arrival Calendar Dimension Table :  We will take all distinct arrival date from dataset and derive all other attributes of this table using that date for example year, month etc. This table can join with fact using arr_dt_cd.
* After that we will be reading USA demographic dataset and try of explore data of this dataset. After that we will load this dataset into one dimension table which can be join with fact table using state code.
* We will read temprature dataset and will identify descrepencies in data and this data will be loaded with required columns in one dimension table which will be having data related country and country code and this can be join with fact table using country code.
* We will read airport file which contain details related to port we will try to explore it and it will be loaded in one dimension table and it can join with fact table using iata_cd.

After completion of above steps we can do analytics on data and extract some insights from data. For above process we will be using aws S3 to store the data and aws EMR (spark) to process the data.

#### Step 2 : Explore and Assess the Data : 
In this part of project we have explored all datasets and try to identify schema of each dataset and extract requitred columns to load target tables. Also, in this step we have tried to identify descrepencies in data and clean the data. For reference please refer Capstone Project Template.ipynb.

#### Step 3.1 : Define the Data Model :
In this part of project we have defind data model of database. In this project we have developed star schema becuase we have one fact one and six dimension table. Also, star schema database has a small number of tables and clear join paths. Small single-table queries, usually of dimension tables, are almost instantaneous. Large join queries that involve multiple tables take only seconds or minutes to run. A star schema is easy to understand and navigate, with dimensions joined only through the fact table. These joins are more significant to the end user, because they represent the fundamental relationship between parts of the underlying business. Users can also browse dimension table attributes before constructing a query.

So as above explantion is fits with our requirement we have used star schema as this type of schema is easy to understand and required less number of joins to do analysis and extract insights from data.
* Conceptual Data Modelling :  For this please refer Conceptual_Data_Model.xlsx
* Logical Data Modelling : Please find (Database ER diagram.png) diagram and details given below.
  * In this database we have 7 tables (1 fact table and 6 dimension tables). fact table (fact_usa_immigration) is developed from immigration dataset which contain all factual details required to analyze data and it contains below columns.
  * Visa dimension table (lu_visa) and arrival calendar (lu_arrival_calendar) deimnesion table will also be loaded from immigration file only and they can join with fact table using visa_type_id and arr_dt_cd respactively.
  * lu_usa_demographics table will be loaded from USA Demographics file and it can join with fact table using state_cd.
  * lu_country table will be loaded from temprature file and i94res mapping file and it can join with fact table using country_cd.
  * lu_port table will be loaded from airport file and it can join with fact table using iata_cd. Also, lu_travel_mode table is loaded from i94mode mapping file and it can join with fact table using mode_id.
* Physical Data Modelling : In this part we have written Create table SQL script to create tables. For reference please find create_table.sql.

#### Step 3.2 Mapping Data Pipeline : 
* Read USA damographic file, clean it and load it in lu_usa_demographics parquet
* Read teprature file, clean it, combine it with i94res.csv file and load it in lu_country parquet
* Read airport file, clean it load it in lu_port parquet.
* Read Immigration file, clean it as per requirement for each parquet and load them in lu_arrival_calendar, lu_visa and fact_usa_immigration parquests.

#### Stpe 4 : Run ETL to Model the Data:
Move required scripts in aws EMR cluster and run below command to execute python script and model the data.
```spark-submit etl_process.py```

### Data Analytics :
We have created crawler in aws glue to create database and table in aws athena which will refer parquet data stored in S3 bucket and we can write and query to analyze data and extract insights for that data.

#### Gender Variance Calculation
**SQL**
```
SELECT 
case when gender = 'M' then 'Male' 
     when gender = 'F' then 'Female' end as gender,
sum(count) as number_of_visitors
FROM "usa_immigration_db"."fact_usa_immigration_parquet" 
group by gender
```
**Output**
gender | number_of_visitors
--- | ---
Male |  91563
Female | 86418

#### Number of visitors with port type
**SQL**
```
SELECT  
b.port_type,
sum(a.count) number_of_visitors
FROM "usa_immigration_db"."fact_usa_immigration_parquet" a
join "usa_immigration_db"."lu_port_parquet" b
on a.port_cd = b.iata_cd 
group by b.port_type
```
**Output**
port_type | number_of_visitors
--- | ---
small_airport | 18534
medium_airport | 23941
large_airport | 63445
closed | 9817
seaplane_base | 25

#### Age Range Analytics
***SQL***
```
SELECT 
case when age_of_respondent >= 0 and age_of_respondent <= 12 then 'Childern'
when age_of_respondent > 12 and age_of_respondent <= 60 then 'Youngster'
when age_of_respondent > 60 then 'Elder' end as type_visitor,
sum(count) as number_of_visitors
FROM "usa_immigration_db"."fact_usa_immigration_parquet" 
group by 
case when age_of_respondent >= 0 and age_of_respondent <= 12 then 'Childern'
when age_of_respondent > 12 and age_of_respondent <= 60 then 'Youngster'
when age_of_respondent > 60 then 'Elder' end 
```
***Output***
type_visitor | number_of_visitors
--- | ---
Elder | 23122
Childern | 12051
Youngster | 142808

#### Top 5 Visa Types Used by Visitors
***SQL***
```
SELECT  
visa_type,
sum(count) as number_of_visitors
FROM "usa_immigration_db"."fact_usa_immigration_parquet" a
join "usa_immigration_db"."lu_visa_parquet" b
on a.visa_type_id = b.visa_type_id 
group by visa_type
order by number_of_visitors desc
limit 5
```
***Output***
visa_type | number_of_visitors
--- | ---
WT | 89228
B2 | 59050
WB | 12855
B1 | 10777
F1 | 3903