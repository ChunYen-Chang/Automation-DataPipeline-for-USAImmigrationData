# create and set path to a schema
schema_create_query = 'CREATE SCHEMA IF NOT EXISTS USAImmigration'
schema_set_path_query = 'SET search_path TO USAImmigration'

# drop tables
I94Immigration_form_drop = 'DROP TABLE IF EXISTS "I94Immigration_form"'
I94CIT_I94RES_Code_drop = 'DROP TABLE IF EXISTS "I94CIT_I94RES_Code"'
I94Addr_Code_drop = 'DROP TABLE IF EXISTS "I94Addr_Code"'
US_Demography_by_State_drop = 'DROP TABLE IF EXISTS "US_Demography_by_State"'
I94Port_Code_drop = 'DROP TABLE IF EXISTS "I94Port_Code"'
I94Mode_Code_drop = 'DROP TABLE IF EXISTS "I94Mode_Code"'
I94Visa_Code_drop = 'DROP TABLE IF EXISTS "I94Visa_Code"'
US_Demography_by_City_drop = 'DROP TABLE IF EXISTS "US_Demography_by_City"'
Airport_Information_drop = 'DROP TABLE IF EXISTS "Airport_Information"'

# create tables
I94Immigration_form_table_create = (
'''CREATE TABLE IF NOT EXISTS public.I94Immigration_form (
    cicid numeric(18,0),
    i94yr numeric(18,0),
    i94mon numeric(18,0),
    i94cit numeric(18,0),
    i94res numeric(18,0),
    i94port varchar(256),
    i94mode numeric(18,0),
    i94addr varchar(256),
    i94bir numeric(18,0),
    i94visa numeric(18,0),
    visapost varchar(256),
    occup varchar(256),
    entdepa varchar(256),
    entdepd varchar(256),
    entdepu varchar(256),    
    matflag varchar(256),
    biryear numeric(18,0),    
    gender varchar(256),
    airline varchar(256),
    admnum numeric(18,0),
    fltno varchar(256),
    visatype varchar(256),
    arrival_date varchar(256),
    departure_date varchar(256),
    allowedtostay_date varchar(256),
    PRIMARY KEY (cicid, i94yr, i94mon),
    FOREIGN KEY (i94cit) REFERENCES I94CIT_I94RES_Code (i94CIT_i94RES_code),
    FOREIGN KEY (i94res) REFERENCES I94CIT_I94RES_Code (i94CIT_i94RES_code),
    FOREIGN KEY (i94port) REFERENCES I94Port_Code (i94port_code),
    FOREIGN KEY (i94mode) REFERENCES I94Mode_Code (i94mode_code),
    FOREIGN KEY (i94addr) REFERENCES I94Addr_Code (i94addr_code),
    FOREIGN KEY (i94visa) REFERENCES I94Visa_Code (i94visa_code)
);''')


I94CIT_I94RES_table_create = (
'''CREATE TABLE IF NOT EXISTS public.I94CIT_I94RES_Code (
    i94CIT_i94RES_code numeric(18,0),
    country varchar(256),
    PRIMARY KEY (i94CIT_i94RES_code)
);''')

I94Addr_Code_table_create = (
'''CREATE TABLE IF NOT EXISTS public.I94Addr_Code (
    i94addr_code varchar(256),
    state varchar(256),
    PRIMARY KEY (i94addr_code)
);''')

US_Demography_by_State_table_create = (
'''CREATE TABLE IF NOT EXISTS public.US_Demography_by_State (
    state varchar(256),
    median_age numeric(18,0),
    male_population numeric(18,0),
    female_population numeric(18,0),
    total_population numeric(18,0),
    number_of_veterans numeric(18,0),
    foreign_born numeric(18,0),
    average_household_size numeric(18,0),
    count numeric(18,0),
    race varchar(256),
    PRIMARY KEY (state)
);''')

I94Port_Code_table_create = (
'''CREATE TABLE IF NOT EXISTS public.I94Port_Code (
    i94port_code varchar(256),
    state varchar(256),
    city varchar(256),
    PRIMARY KEY (i94port_code)
);''')

I94Mode_Code_table_create = (
'''CREATE TABLE IF NOT EXISTS public.I94Mode_Code (
    i94mode_code numeric(18,0),
    transportation varchar(256),
    PRIMARY KEY (i94mode_code)
);''')

I94Visa_Code_table_create = (
'''CREATE TABLE IF NOT EXISTS public.I94Visa_Code (
    i94visa_code numeric(18,0),
    visatype varchar(256),
    PRIMARY KEY (i94visa_code)
);''')

US_Demography_by_City_table_create = (
'''CREATE TABLE IF NOT EXISTS public.US_Demography_by_City (
    city varchar(256),
    state_code varchar(256),
    median_age numeric(18,0),
    male_population numeric(18,0),
    female_population numeric(18,0),
    total_population numeric(18,0),
    number_of_veterans numeric(18,0),
    foreign_born numeric(18,0),
    average_household_size numeric(18,0),
    count numeric(18,0),
    race varchar(256),
    PRIMARY KEY (city)
);''')

Airport_Information_table_create = (
'''CREATE TABLE IF NOT EXISTS public.Airport_Information (
    ident varchar(256),
    airport_type varchar(256),
    name varchar(256),
    elevation_ft varchar(256),
    continent varchar(256),
    iso_country varchar(256),
    iso_region varchar(256),
    municipality varchar(256),
    gps_code varchar(256),
    iata_code varchar(256),
    local_code varchar(256),
    coordinates varchar(256),
    PRIMARY KEY (ident)
);''')

# insert data into tables
I94Addr_table_insert = ("""COPY I94Addr_Code 
                            from 's3://udacityfikrusnk/I94ADDR.csv'
                            credentials 'aws_access_key_id={};aws_secret_access_key={}'
                            region 'us-west-2'
                            FORMAT CSV
                            delimiter ','
                            IGNOREHEADER 1;
                        """)


I94CITandI94RES_table_insert = ("""COPY I94CIT_I94RES_Code 
                            from 's3://udacityfikrusnk/I94CITandI94RES.csv'
                            credentials 'aws_access_key_id={};aws_secret_access_key={}'
                            region 'us-west-2'
                            FORMAT CSV
                            delimiter ','
                            IGNOREHEADER 1;
                        """)


I94Mode_table_insert = ("""COPY I94Mode_Code
                            from 's3://udacityfikrusnk/I94MODE.csv'
                            credentials 'aws_access_key_id={};aws_secret_access_key={}'
                            region 'us-west-2'
                            FORMAT CSV
                            delimiter ','
                            IGNOREHEADER 1;
                        """)


I94Port_table_insert = ("""COPY I94Port_Code
                            from 's3://udacityfikrusnk/I94PORT.csv'
                            credentials 'aws_access_key_id={};aws_secret_access_key={}'
                            region 'us-west-2'
                            FORMAT CSV
                            delimiter ','
                            IGNOREHEADER 1;
                        """)


I94Visa_table_insert = ("""COPY I94Visa_Code 
                            from 's3://udacityfikrusnk/I94VISA.csv'
                            credentials 'aws_access_key_id={};aws_secret_access_key={}'
                            region 'us-west-2'
                            FORMAT CSV
                            delimiter ','
                            IGNOREHEADER 1;
                        """)


Airport_Information_table_insert = ("""COPY Airport_Information
                            from 's3://udacityfikrusnk/airport-codes-after-process.csv'
                            credentials 'aws_access_key_id={};aws_secret_access_key={}'
                            region 'us-west-2'
                            FORMAT CSV
                            delimiter ','
                            IGNOREHEADER 1;
                        """)


US_Demography_by_City_table_insert = ("""COPY US_Demography_by_City
                            from 's3://udacityfikrusnk/us-demographics-by-city.csv'
                            credentials 'aws_access_key_id={};aws_secret_access_key={}'
                            region 'us-west-2'
                            FORMAT CSV
                            delimiter ','
                            IGNOREHEADER 1;
                        """)


US_Demography_by_State_table_insert = ("""COPY US_Demography_by_State
                            from 's3://udacityfikrusnk/us-demographics-by-state.csv'
                            credentials 'aws_access_key_id={};aws_secret_access_key={}'
                            region 'us-west-2'
                            FORMAT CSV
                            delimiter ','
                            IGNOREHEADER 1;
                        """)


I94Immigration_form_table_insert = ("""COPY I94Immigration_form
                            from 's3://udacityfikrusnk/i94_immigration_after/jan16/'
                            credentials 'aws_access_key_id={};aws_secret_access_key={}'
                            region 'us-west-2'
                            FORMAT CSV
                            delimiter ','
                            IGNOREHEADER 1;
                        """)