CREATE TABLE IF NOT EXISTS public.staging_flights (
      DEPARTURES_SCHEDULED DECIMAL(12,2),
      DEPARTURES_PERFORMED DECIMAL(12,2),
      PAYLOAD DECIMAL(12,2),
      SEATS DECIMAL(12,2),
      PASSENGERS DECIMAL(12,2),
      FREIGHT DECIMAL(12,2),
      MAIL DECIMAL(12,2),
      DISTANCE DECIMAL(12,2),
      RAMP_TO_RAMP DECIMAL(12,2),
      AIR_TIME DECIMAL(12,2),
      UNIQUE_CARRIER TEXT,
      AIRLINE_ID INT,
      UNIQUE_CARRIER_NAME TEXT,
      UNIQUE_CARRIER_ENTITY TEXT,
      REGION CHAR(1),
      CARRIER CHAR(3),
      CARRIER_NAME TEXT,
      CARRIER_GROUP INT,
      CARRIER_GROUP_NEW INT,
      ORIGIN_AIRPORT_ID INT,
      ORIGIN_AIRPORT_SEQ_ID INT,
      ORIGIN_CITY_MARKET_ID INT,
      ORIGIN CHAR(3),
      ORIGIN_CITY_NAME TEXT,
      ORIGIN_STATE_ABR CHAR(2),
      ORIGIN_STATE_FIPS INT,
      ORIGIN_STATE_NM TEXT,
      ORIGIN_COUNTRY CHAR(2),
      ORIGIN_COUNTRY_NAME TEXT,
      ORIGIN_WAC INT,
      DEST_AIRPORT_ID INT,
      DEST_AIRPORT_SEQ_ID INT,
      DEST_CITY_MARKET_ID INT,
      DEST CHAR(3),
      DEST_CITY_NAME TEXT,
      DEST_STATE_ABR CHAR(2),
      DEST_STATE_FIPS INT,
      DEST_STATE_NM TEXT,
      DEST_COUNTRY CHAR(2),
      DEST_COUNTRY_NAME TEXT,
      DEST_WAC INT,
      AIRCRAFT_GROUP INT,
      AIRCRAFT_TYPE INT,
      AIRCRAFT_CONFIG INT,
      YEAR INT,
      QUARTER INT,
      MONTH INT,
      DISTANCE_GROUP INT,
      CLASS CHAR(1),
      DATA_SOURCE CHAR(2)
    );


    CREATE TABLE IF NOT EXISTS public.staging_aircraft_code (
      AC_TYPEID INT,
      AC_GROUP INT,
      SSD_NAME VARCHAR(50),
      MANUFACTURER VARCHAR(50),
      LONG_NAME VARCHAR(50),
      SHORT_NAME VARCHAR(50),
      BEGIN_DATE VARCHAR(50),
      END_DATE VARCHAR(50)
    );

    
    CREATE TABLE IF NOT EXISTS public.staging_aircraft_group (
      AC_GROUP INT NOT NULL,
      AC_GROUP_DESCRIPTION TEXT
    );

    CREATE TABLE IF NOT EXISTS public.staging_aircraft_configuration (
      AC_CONFIG INT NOT NULL,
      AC_CONFIG_DESCRIPTION TEXT
    );

    CREATE TABLE IF NOT EXISTS public.dim_aircraft_code (
      ac_typeid INT NOT NULL,
      ac_group INT,
      ssd_name VARCHAR(50),
      manufacturer VARCHAR(50),
      long_name VARCHAR(50),
      short_name VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS public.dim_aircraft_group (
      ac_group INT,
      ac_group_description TEXT
    );

    
    CREATE TABLE IF NOT EXISTS public.dim_airport_code (
      airport_id INT,
      airport_seq_id INT,
      city_market_id INT,
      airport_code CHAR(3),
      city_name TEXT,
      state_abr CHAR(2),
      state_fips INT,
      state_nm TEXT,
      country CHAR(5),
      country_name TEXT,
      wac INT
    );

    
    CREATE TABLE IF NOT EXISTS public.dim_aircraft_configuration (
      ac_config INT,
      ac_config_description TEXT
    );



    CREATE TABLE IF NOT EXISTS public.fact_flights (
      DEPARTURES_SCHEDULED DECIMAL(12,2),
      DEPARTURES_PERFORMED DECIMAL(12,2),
      PAYLOAD DECIMAL(12,2),
      SEATS DECIMAL(12,2),
      PASSENGERS DECIMAL(12,2),
      FREIGHT DECIMAL(12,2),
      MAIL DECIMAL(12,2),
      DISTANCE DECIMAL(12,2),
      RAMP_TO_RAMP DECIMAL(12,2),
      AIR_TIME DECIMAL(12,2),
      UNIQUE_CARRIER_ENTITY TEXT,
      ORIGIN CHAR(3),
      DEST CHAR(3),
      AIRCRAFT_TYPE INT,
      AIRCRAFT_CONFIG INT,
      YEAR INT,
      QUARTER INT,
      MONTH INT,
      DISTANCE_GROUP INT,
      CLASS CHAR(1),
      DATA_SOURCE CHAR(2)
    );
