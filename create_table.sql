CREATE TABLE lu_usa_demographics(
  id INTEGER NOT NULL PRIMARY KEY,
  city VARCHAR,
  state VARCHAR,
  state_cd VARCHAR,
  race VARCHAR,
  median_age DECIMAL,
  male_population INTEGER,
  female_population INTEGER,
  total_population INTEGER,
  number_of_veterans INTEGER,
  foreign_born INTEGER,
  avg_house_hold_size DECIMAL,
  count INTEGER
);

CREATE TABLE lu_arrival_calendar(
  arr_dt_cd INTEGER NOT NULL PRIMARY KEY,
  arr_date DATETIME,
  arr_day INTEGER,
  arr_month INTEGER,
  arr_year INTEGER,
  arr_week INTEGER,
  arr_weekday VARCHAR
);

CREATE TABLE lu_country(
  country_cd INTEGER NOT NULL PRIMARY KEY,
  country_nm VARCHAR,
  avg_temprature DECIMAL
);

CREATE TABLE lu_visa(
  visa_type_id VARCHAR NOT NULL PRIMARY KEY,
  visa_type VARCHAR
);

CREATE TABLE lu_port(
  iata_cd VARCHAR NOT NULL PRIMARY KEY,
  port_type VARCHAR,
  port_name VARCHAR,
  elevation INTEGER,
  continet VARCHAR,
  iso_country VARCHAR,
  iso_region VARCHAR,
  municipality VARCHAR,
  coordinates VARCHAR
);

CREATE TABLE lu_travel_mode(
  mode_id INTEGER NOT NULL PRIMARY KEY,
  mode_type VARCHAR
);

CREATE TABLE fact_usa_immigration(
  cic_id INTEGER NOT NULL PRIMARY KEY,
  visa_type_id INTEGER NOT NULL,
  country_cd INTEGER,
  port_cd VARCHAR,
  arr_dt_cd INTEGER,
  mode_id INTEGER,
  state_cd VARCHAR,
  depdate INTEGER,
  age_of_respondent INTEGER,
  count INTEGER,
  birth_year INTEGER,
  gender VARCHAR,
  airline VARCHAR,
  fltno VARCHAR
);
