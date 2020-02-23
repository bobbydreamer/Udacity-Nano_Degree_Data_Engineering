import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('./aws/aws-capstone.cfg')

#print(config.sections())
#print(config.get('LOCAL', 'LOG_LOCAL_DATA'))

# Reading config & initializing variables
details_dict = {}
new_dict = {}
for section in config.sections():
    dic = dict(config.items(section))
    if len(dic) != 0 :
        #print( '{} - {} : {}'.format(len(dic), section, dic) )
        details_dict.update(dic)

        for k, v in details_dict.items():
            k = k.upper()
            #print('{} : {}'.format(k,v))
            new_dict[k] = v

            #print(new_dict)

            for k, v in new_dict.items():
                globals()[k] = v

                #print(' LOG_LOCAL_DATA = {}'.format(LOG_LOCAL_DATA))
    
    
# CREATE SCHEMA 
create_schema = {
    "query": "create schema if not exists capstone;",
    "message": "CREATE schema"
}

# DROP SCHEMA 
drop_schema = {
    "query": "drop schema if exists capstone;",
    "message": "DROP schema"    
}

# SET SCHEMA PATH
set_schema = {
    "query": "SET search_path = capstone;",
    "message": "Setting the schema path"
}

# DROP STAGING TABLES
staging_ids_drop = {
    "query": "DROP TABLE IF EXISTS staging_ids;",
    "message": "DROP TABLE staging_ids"
}

staging_visatype_drop = {
    "query": "DROP TABLE IF EXISTS staging_visatype ;",
    "message": "DROP TABLE staging_visatype "
}

staging_ac_drop = {
    "query": "DROP TABLE IF EXISTS staging_ac;",
    "message": "DROP TABLE staging_ac"
}

staging_USPoE_drop = {
    "query": "DROP TABLE IF EXISTS staging_USPoE;",
    "message": "DROP TABLE staging_USPoE"
}

staging_uszips_drop = {
    "query": "DROP TABLE IF EXISTS staging_uszips;",
    "message": "DROP TABLE staging_uszips"
}

staging_i94countrycode_drop = {
    "query": "DROP TABLE IF EXISTS staging_i94countrycode;",
    "message": "DROP TABLE staging_i94countrycode"
}

staging_usdp_drop = {
    "query": "DROP TABLE IF EXISTS staging_usdp;",
    "message": "DROP TABLE staging_usdp"
}

staging_usdr_drop = {
    "query": "DROP TABLE IF EXISTS staging_usdr ;",
    "message": "DROP TABLE staging_usdr "
}

staging_temper_drop = {
    "query": "DROP TABLE IF EXISTS staging_temper ;",
    "message": "DROP TABLE staging_temper "
}

staging_alc_drop = {
    "query": "DROP TABLE IF EXISTS staging_alc ;",
    "message": "DROP TABLE staging_alc "
}

staging_visapost_drop = {
    "query": "DROP TABLE IF EXISTS staging_visapost ;",
    "message": "DROP TABLE staging_visapost "
}

staging_alpha2countrycode_drop = {
    "query": "DROP TABLE IF EXISTS staging_alpha2countrycode ;",
    "message": "DROP TABLE staging_alpha2countrycode "
}

staging_iap_drop = {
    "query": "DROP TABLE IF EXISTS staging_iap ;",
    "message": "DROP TABLE staging_iap "
}

staging_wc_drop = {
    "query": "DROP TABLE IF EXISTS staging_wc ;",
    "message": "DROP TABLE staging_wc "
}

staging_USstatecode_drop = {
    "query": "DROP TABLE IF EXISTS staging_USstatecode;",
    "message": "DROP TABLE staging_USstatecode"
}

# DROP FACT & DIM TABLES
fact_ids_drop = {
    "query": "DROP TABLE IF EXISTS fact_ids;",
    "message": "DROP TABLE fact_ids"
}

dim_visatype_drop = {
    "query": "DROP TABLE IF EXISTS dim_visatype ;",
    "message": "DROP TABLE dim_visatype "
}

dim_ac_drop = {
    "query": "DROP TABLE IF EXISTS dim_ac;",
    "message": "DROP TABLE dim_ac"
}

dim_USPoE_drop = {
    "query": "DROP TABLE IF EXISTS dim_USPoE;",
    "message": "DROP TABLE dim_USPoE"
}

dim_uszips_drop = {
    "query": "DROP TABLE IF EXISTS dim_uszips;",
    "message": "DROP TABLE dim_uszips"
}

dim_i94countrycode_drop = {
    "query": "DROP TABLE IF EXISTS dim_i94countrycode;",
    "message": "DROP TABLE dim_i94countrycode"
}

dim_usdp_drop = {
    "query": "DROP TABLE IF EXISTS dim_usdp;",
    "message": "DROP TABLE dim_usdp"
}

dim_usdr_drop = {
    "query": "DROP TABLE IF EXISTS dim_usdr ;",
    "message": "DROP TABLE dim_usdr "
}

dim_temper_drop = {
    "query": "DROP TABLE IF EXISTS dim_temper ;",
    "message": "DROP TABLE dim_temper "
}

dim_alc_drop = {
    "query": "DROP TABLE IF EXISTS dim_alc ;",
    "message": "DROP TABLE dim_alc "
}

dim_visapost_drop = {
    "query": "DROP TABLE IF EXISTS dim_visapost ;",
    "message": "DROP TABLE dim_visapost "
}

dim_alpha2countrycode_drop = {
    "query": "DROP TABLE IF EXISTS dim_alpha2countrycode ;",
    "message": "DROP TABLE dim_alpha2countrycode "
}

dim_iap_drop = {
    "query": "DROP TABLE IF EXISTS dim_iap ;",
    "message": "DROP TABLE dim_iap "
}

dim_wc_drop = {
    "query": "DROP TABLE IF EXISTS dim_wc ;",
    "message": "DROP TABLE dim_wc "
}

dim_USstatecode_drop = {
    "query": "DROP TABLE IF EXISTS dim_USstatecode;",
    "message": "DROP TABLE dim_USstatecode"
}

# CREATE Staging TABLES
staging_ids_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_ids" (
          "i94_id"   INTEGER IDENTITY(0,1),
          "CoC" VARCHAR,
          "CoR" VARCHAR,
          "PoE" VARCHAR,
          "landing_state" VARCHAR,
          "age" VARCHAR,
          "visa_issued_in" VARCHAR,
          "occup" VARCHAR,
          "biryear" VARCHAR,
          "gender" VARCHAR,
          "airline" VARCHAR,
          "admnum" VARCHAR,
          "fltno" VARCHAR,
          "visatype" VARCHAR,
          "arrival_mode" VARCHAR,
          "visit_purpose" VARCHAR,
          "arrival_dt" TIMESTAMP,
          "departure_dt" TIMESTAMP,
          "DaysinUS" DECIMAL(16,5),
          "added_to_i94" TIMESTAMP,
          "allowed_until" TIMESTAMP,
          "entry_exit" VARCHAR,
          "month" INTEGER
        );
        """,
    "message": "CREATE TABLE staging_ids"
}

staging_visatype_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_visatype" (
        "visatype" VARCHAR,
        "description" VARCHAR
        );
        """,
    "message": "CREATE TABLE staging_visatype"
}

staging_ac_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_ac" (
        "type" VARCHAR,
        "name" VARCHAR,
        "elevation_ft" DECIMAL(16,5),
        "continent" DECIMAL(16,5),
        "iso_country" VARCHAR,
        "iso_region" VARCHAR,
        "municipality" VARCHAR,
        "iata_code" VARCHAR,
        "local_code" VARCHAR,
        "coordinates" VARCHAR,
        "longitude" VARCHAR,
        "latitude" VARCHAR,
        "state" VARCHAR,
        "facilities" VARCHAR
        );
        """,
    "message": "CREATE TABLE staging_ac"
}

staging_USPoE_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_USPoE" (
        "code" VARCHAR,
        "citystate" VARCHAR,
        "city" VARCHAR,
        "state" VARCHAR
        );
        """,
    "message": "CREATE TABLE staging_USPoE"
}

staging_uszips_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_uszips" (
        "zip" INTEGER,
        "lat" DECIMAL(16,5),
        "lng" DECIMAL(16,5),
        "city" VARCHAR,
        "state_id" VARCHAR,
        "state_name" VARCHAR,
        "population" INTEGER,
        "density" DECIMAL(16,5),
        "county_fips" INTEGER,
        "county_name" VARCHAR
        );
        """,
    "message": "CREATE TABLE staging_uszips"
}

staging_i94countrycode_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_i94countrycode" (
        "code" VARCHAR,
        "country" VARCHAR
        );
        """,
    "message": "CREATE TABLE staging_i94countrycode"
}

staging_usdp_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_usdp" (
        "State" VARCHAR,
        "City" VARCHAR,
        "Median_Age" DECIMAL(16,5),
        "State_Code" VARCHAR,
        "Male_Population" INTEGER,
        "Female_Population" INTEGER,
        "Total_Population" INTEGER,
        "Number_of_Veterans" INTEGER,
        "Foreign_born" INTEGER
        );
        """,
    "message": "CREATE TABLE staging_usdp"
}

staging_usdr_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_usdr" (
        "State" VARCHAR,
        "City" VARCHAR,
        "State_Code" VARCHAR,
        "American_Indian_and_Alaska_Native" INTEGER,
        "Asian" INTEGER,
        "Black_or_African_American" INTEGER,
        "Hispanic_or_Latino" INTEGER,
        "White" INTEGER
        );
        """,
    "message": "CREATE TABLE staging_usdr"
}

staging_temper_table_create = {
    "query": """    
        CREATE TABLE IF NOT EXISTS "staging_temper" (
        "dt" TIMESTAMP,
          "AverageTemperature" DECIMAL(16,5),
          "City" VARCHAR,
          "Country" VARCHAR
        );    
        """,
    "message": "CREATE TABLE staging_temper"
}

staging_alc_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_alc" (
        "code" VARCHAR,
        "description" VARCHAR
        );
        """,
    "message": "CREATE TABLE staging_alc"
}

staging_visapost_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_visapost" (
        "location" VARCHAR,
        "code" VARCHAR
        );
        """,
    "message": "CREATE TABLE staging_visapost"
}

staging_alpha2countrycode_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_alpha2countrycode" (
        "country" VARCHAR,
        "alpha2" VARCHAR,
        "alpha3" VARCHAR,
        "Num" INTEGER,
        "ITU" VARCHAR,
        "GEC" VARCHAR,
        "IOC" VARCHAR,
        "FIFA" VARCHAR,
        "DS" VARCHAR,
        "WMO" VARCHAR,
        "GAUL" VARCHAR,
        "MARC" VARCHAR,
        "dial" VARCHAR,
        "independent" VARCHAR,
        "VisaRequired" VARCHAR
        );
        """,
    "message": "CREATE TABLE staging_alpha2countrycode"
}

staging_iap_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_iap" (
        "Location" VARCHAR,
        "Airport" VARCHAR,
        "IATA" VARCHAR        
        );
        """,
    "message": "CREATE TABLE staging_iap"
}

staging_wc_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_wc" (
        "city" VARCHAR,
          "city_ascii" VARCHAR,
          "lat" DECIMAL(16,5),
          "lng" DECIMAL(16,5),
          "country" VARCHAR,
          "iso2" VARCHAR,
          "iso3" VARCHAR,
          "admin_name" VARCHAR,
          "capital" VARCHAR,
          "population" DECIMAL(16,5),
          "id" INTEGER
        );    
        """,
    "message": "CREATE TABLE staging_wc"
}

staging_USstatecode_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_USstatecode" (
        "code" VARCHAR,
        "state" VARCHAR,
        "status" VARCHAR
        );    
        """,
    "message": "CREATE TABLE staging_USstatecode"
}

# CREATE Fact & Dim TABLES
fact_ids_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "fact_ids" (
        "i94_id"   INTEGER IDENTITY(0,1),
          "CoC" VARCHAR,
          "CoR" VARCHAR,
          "PoE" VARCHAR,
          "landing_state" VARCHAR,
          "age" VARCHAR,
          "visa_issued_in" VARCHAR,
          "occup" VARCHAR,
          "biryear" VARCHAR,
          "gender" VARCHAR,
          "airline" VARCHAR,
          "admnum" VARCHAR,
          "fltno" VARCHAR,
          "visatype" VARCHAR,
          "arrival_mode" VARCHAR,
          "visit_purpose" VARCHAR,
          "arrival_dt" TIMESTAMP,
          "departure_dt" TIMESTAMP,
          "DaysinUS" DECIMAL(16,5),
          "added_to_i94" TIMESTAMP,
          "allowed_until" TIMESTAMP,
          "entry_exit" VARCHAR,
        CONSTRAINT fact_ids_pk PRIMARY KEY ("i94_id")
        );
        """,
    "message": "CREATE TABLE fact_ids"
}

dim_visatype_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_visatype" (
        "visatype" VARCHAR,
        "description" VARCHAR,
        CONSTRAINT dim_visatype_pk PRIMARY KEY ("visatype")
        );    
        """,
    "message": "CREATE TABLE dim_visatype"
}

dim_ac_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_ac" (
        "type" VARCHAR,
        "name" VARCHAR,
        "elevation_ft" DECIMAL(16,5),
        "continent" DECIMAL(16,5),
        "iso_country" VARCHAR,
        "iso_region" VARCHAR,
        "municipality" VARCHAR,
        "iata_code" VARCHAR,
        "local_code" VARCHAR,
        "coordinates" VARCHAR,
        "longitude" VARCHAR,
        "latitude" VARCHAR,
        "state" VARCHAR,
        "facilities" VARCHAR,
        CONSTRAINT dim_ac_pk PRIMARY KEY ("type", "name")
        );    
        """,
    "message": "CREATE TABLE dim_ac"
}

dim_USPoE_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_USPoE" (
        "code" VARCHAR,
        "citystate" VARCHAR,
        "city" VARCHAR,
        "state" VARCHAR,
        CONSTRAINT dim_USPoE_pk PRIMARY KEY ("code")
        );
        """,
    "message": "CREATE TABLE dim_USPoE"
}

dim_uszips_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_uszips" (
        "zip" INTEGER,
        "lat" DECIMAL(16,5),
        "lng" DECIMAL(16,5),
        "city" VARCHAR,
        "state_id" VARCHAR,
        "state_name" VARCHAR,
        "population" INTEGER,
        "density" DECIMAL(16,5),
        "county_fips" INTEGER,
        "county_name" VARCHAR,
        CONSTRAINT dim_uszips_pk PRIMARY KEY ("zip")
        );    
        """,
    "message": "CREATE TABLE dim_uszips"
}

dim_i94countrycode_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_i94countrycode" (
        "code" VARCHAR,
        "country" VARCHAR,
        CONSTRAINT dim_i94countrycode_pk PRIMARY KEY ("code")
        );    
        """,
    "message": "CREATE TABLE dim_i94countrycode"
}

dim_usdp_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_usdp" (
        "State" VARCHAR,
        "City" VARCHAR,
        "Median_Age" DECIMAL(16,5),
        "State_Code" VARCHAR,
        "Male_Population" INTEGER,
        "Female_Population" INTEGER,
        "Total_Population" INTEGER,
        "Number_of_Veterans" INTEGER,
        "Foreign_born" INTEGER,
        CONSTRAINT dim_usdp_pk PRIMARY KEY ("State", "City")
        );
        """,
    "message": "CREATE TABLE dim_usdp"
}

dim_usdr_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_usdr" (
        "State" VARCHAR,
        "City" VARCHAR,
        "State_Code" VARCHAR,
        "American_Indian_and_Alaska_Native" INTEGER,
        "Asian" INTEGER,
        "Black_or_African_American" INTEGER,
        "Hispanic_or_Latino" INTEGER,
        "White" INTEGER,
        CONSTRAINT dim_usdr_pk PRIMARY KEY ("State", "City")
        );    
        """,
    "message": "CREATE TABLE dim_usdr"
}

dim_temper_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_temper" (
        "dt" TIMESTAMP,
          "AverageTemperature" DECIMAL(16,5),
          "City" VARCHAR,
          "Country" VARCHAR,
        CONSTRAINT dim_temper_pk PRIMARY KEY ("dt", "City", "Country")
        );        
        """,
    "message": "CREATE TABLE dim_temper"
}

dim_alc_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_alc" (
        "code" VARCHAR,
        "description" VARCHAR,
        CONSTRAINT dim_alc_pk PRIMARY KEY ("code")
        );
        """,
    "message": "CREATE TABLE dim_alc"
}

dim_visapost_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_visapost" (
        "location" VARCHAR,
        "code" VARCHAR,
        CONSTRAINT dim_visapost_pk PRIMARY KEY ("code")
        );    
        """,
    "message": "CREATE TABLE dim_visapost"
}

dim_alpha2countrycode_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_alpha2countrycode" (
        "country" VARCHAR,
        "alpha2" VARCHAR,
        "alpha3" VARCHAR,
        "Num" INTEGER,
        "ITU" VARCHAR,
        "GEC" VARCHAR,
        "IOC" VARCHAR,
        "FIFA" VARCHAR,
        "DS" VARCHAR,
        "WMO" VARCHAR,
        "GAUL" VARCHAR,
        "MARC" VARCHAR,
        "dial" VARCHAR,
        "independent" VARCHAR,
        "VisaRequired" VARCHAR,
        CONSTRAINT dim_alpha2countrycode_pk PRIMARY KEY ("country")
        );    
        """,
    "message": "CREATE TABLE dim_alpha2countrycode"
}

dim_iap_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_iap" (
        "Location" VARCHAR,
        "Airport" VARCHAR,
        "IATA" VARCHAR,
        CONSTRAINT dim_iap_pk PRIMARY KEY ("Airport", "IATA")
        );
        """,
    "message": "CREATE TABLE dim_iap"
}

dim_wc_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_wc" (
        "city" VARCHAR,
          "city_ascii" VARCHAR,
          "lat" DECIMAL(10,6),
          "lng" DECIMAL(10,6),
          "country" VARCHAR,
          "iso2" VARCHAR,
          "iso3" VARCHAR,
          "admin_name" VARCHAR,
          "capital" VARCHAR,
          "population" DECIMAL(16,5),
          "id" INTEGER,
          CONSTRAINT dim_wc_pk PRIMARY KEY ("city", "country")
        );    
        """,
    "message": "CREATE TABLE dim_wc"
}

dim_USstatecode_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_USstatecode" (
        "code" VARCHAR,
        "state" VARCHAR,
        "status" VARCHAR,
        CONSTRAINT dim_USstatecode_pk PRIMARY KEY ("code")
        );    
        """,
    "message": "CREATE TABLE dim_USstatecode"
}

# STAGING TABLES
'''
staging_events_copy_query = ("""
    COPY staging_events FROM {}
    CREDENTIALS '{}'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    CSV 
    delimiter ',' 
    IGNOREHEADER 1
    COMPUPDATE OFF REGION 'us-west-2';
    """).format(CLEAN_LOG_DATA, KEYSECRET)

staging_events_copy = {
    "query": staging_events_copy_query,
    "message": "COPY staging_events"
}
'''

# FINAL INSERTs to FACT & DIMENSION tables
dim_visatype_insert = {
    "query": ("""
        insert into capstone.dim_visatype(visatype, description) 
            select distinct  visatype, description 
                from capstone.staging_visatype ;
        """),
    "message": "INSERT INTO dim_visatype TABLE"
}

dim_iap_insert = {
    "query": ("""
        insert into capstone.dim_iap(Location, Airport, IATA)
        select distinct  Location, Airport, IATA
        from capstone.staging_iap ; 
        """),
    "message": "INSERT INTO dim_iap TABLE"
}

dim_usstatecode_insert = {
    "query": ("""
        insert into capstone.dim_usstatecode(code, state, status)
        select distinct  code, state, status
        from capstone.staging_usstatecode ; 
        """),
    "message": "INSERT INTO dim_usstatecode TABLE"
}

dim_uszips_insert = {
    "query": ("""
        insert into capstone.dim_uszips(zip, lat, lng, city, state_id, state_name, population, density, county_fips, county_name)
        select distinct  a.zip, a.lat, a.lng, a.city, a.state_id, a.state_name, a.population, a.density, a.county_fips, a.county_name
        from capstone.staging_uszips a inner join capstone.staging_usstatecode b
        on a.state_id = b.code;    
        """),
    "message": "INSERT INTO dim_uszips TABLE"
}

dim_ac_insert = {
    "query": ("""
        insert into capstone.dim_ac(type, name, elevation_ft, continent, iso_country, iso_region, municipality, iata_code, local_code, coordinates, longitude, latitude, state, facilities)
        select distinct  a.type, a.name, a.elevation_ft, a.continent, a.iso_country, a.iso_region, a.municipality, a.iata_code, a.local_code, a.coordinates, a.longitude, a.latitude, a.state, a.facilities
        from capstone.staging_ac a inner join capstone.staging_iap b on a.iata_code = b.IATA
        inner join capstone.staging_uszips c on a.state = c.state_id;    
        """),
    "message": "INSERT INTO dim_ac TABLE"
}

dim_USPoE_insert = {
    "query": ("""
        insert into capstone.dim_USPoE(code, citystate, city, state)
        select distinct  a.code, a.citystate, a.city, a.state
        from capstone.staging_uspoe a inner join capstone.staging_usstatecode b on a.state = b.code ;    
        """),
    "message": "INSERT INTO dim_USPoE TABLE"
}

dim_alpha2countrycode_insert = {
    "query": ("""
        insert into capstone.dim_alpha2countrycode(country, alpha2, alpha3, Num, ITU, GEC, IOC, FIFA, DS, WMO, GAUL, MARC, dial, independent, VisaRequired)
        select distinct  country, alpha2, alpha3, Num, ITU, GEC, IOC, FIFA, DS, WMO, GAUL, MARC, dial, independent, VisaRequired
        from capstone.staging_alpha2countrycode ;    
        """),
    "message": "INSERT INTO dim_alpha2countrycode TABLE"
}

# Commenting below query as around 41 countries are filtered due to the joins(Major country missed is china, so loading all the data)
'''
dim_i94countrycode_insert = {
    "query": ("""
        insert into capstone.dim_i94countrycode(code, country)
        select distinct  a.code, a.country
        from capstone.staging_i94countrycode a inner join capstone.staging_alpha2countrycode b
        on lower(a.country) = lower(b.country) ;        
        """),
    "message": "INSERT INTO dim_i94countrycode TABLE"
}
'''
        
dim_i94countrycode_insert = {
    "query": ("""
        insert into capstone.dim_i94countrycode(code, country)
        select distinct  a.code, a.country
        from capstone.staging_i94countrycode a ;        
        """),
    "message": "INSERT INTO dim_i94countrycode TABLE"
}


dim_usdp_insert = {
    "query": ("""
        insert into capstone.dim_usdp(State, City, Median_Age, State_Code, Male_Population, Female_Population, Total_Population, Number_of_Veterans, Foreign_born)
        select distinct  a.State, a.City, a.Median_Age, a.State_Code, a.Male_Population, a.Female_Population, a.Total_Population, a.Number_of_Veterans, a.Foreign_born
        from capstone.staging_usdp a inner join capstone.staging_uszips b 
        on a.state_code = b.state_id ;    
        """),
    "message": "INSERT INTO dim_usdp TABLE"
}

dim_usdr_insert = {
    "query": ("""
        insert into capstone.dim_usdr(State, City, State_Code, American_Indian_and_Alaska_Native, Asian, Black_or_African_American, Hispanic_or_Latino, White)
        select distinct  a.State, a.City, a.State_Code, a.American_Indian_and_Alaska_Native, a.Asian, a.Black_or_African_American, a.Hispanic_or_Latino, a.White
        from capstone.staging_usdr a inner join capstone.staging_uszips b 
        on a.state_code = b.state_id ;    
        """),
    "message": "INSERT INTO dim_usdr TABLE"
}

dim_temper_insert = {
    "query": ("""
        insert into capstone.dim_temper(dt, AverageTemperature, City, Country)
        select distinct  a.dt, a.AverageTemperature, a.City, a.Country
        from capstone.staging_temper a inner join capstone.staging_uszips b
        on a.City = b.city ;     
        """),
    "message": "INSERT INTO dim_temper TABLE"
}

dim_wc_insert = {
    "query": ("""
        insert into capstone.dim_wc(city, city_ascii, lat, lng, country, iso2, iso3, admin_name, capital, population, id)
        select distinct  city, city_ascii, lat, lng, country, iso2, iso3, admin_name, capital, population, id
        from capstone.staging_wc ;    
        """),
    "message": "INSERT INTO dim_wc TABLE"
}

dim_alc_insert = {
    "query": ("""
        insert into capstone.dim_alc(code, description)
        select distinct  code, description
        from capstone.staging_alc where code is not null;     
        """),
    "message": "INSERT INTO dim_alc TABLE"
}

dim_visapost_insert = {
    "query": ("""
        insert into capstone.dim_visapost(location, code)
        select distinct  location, code
        from capstone.staging_visapost ;     
        """),
    "message": "INSERT INTO dim_visapost TABLE"
}

fact_ids_insert = {
    "query": ("""
        insert into capstone.fact_ids(CoC, CoR, PoE, landing_state, age, visa_issued_in, occup, biryear, gender, airline, admnum, fltno, visatype, arrival_mode, visit_purpose, arrival_dt, departure_dt, DaysinUS, added_to_i94, allowed_until, entry_exit)
        select distinct  CoC, CoR, PoE, landing_state, age, visa_issued_in, occup, biryear, gender, airline, admnum, fltno, visatype, arrival_mode, visit_purpose, arrival_dt, departure_dt, DaysinUS, added_to_i94, allowed_until, entry_exit
        from capstone.staging_ids 
        where CoC in (select distinct  code from capstone.staging_i94countrycode)
        and CoR in (select distinct  code from capstone.staging_i94countrycode)
        and PoE in (select distinct  code from capstone.staging_uspoe)
        and landing_state in (select distinct  state_id from capstone.staging_uszips)
        and visa_issued_in in (select distinct  code from capstone.staging_visapost)
        and airline in (select distinct  code from capstone.staging_alc)
        and visatype in (select distinct  visatype from capstone.staging_visatype)    
        """),
    "message": "INSERT INTO fact_ids TABLE"
}

# count rows 
'''
count_staging_events = {
    "query" : ("""
    SELECT COUNT(*) FROM staging_events
    """),
    "message" : "Rows in Staging Events"
}
'''

# QUERY LISTS
schema_queries = [create_schema, drop_schema, set_schema]

create_table_queries = [staging_ids_table_create, staging_visatype_table_create, staging_ac_table_create, staging_USPoE_table_create, staging_uszips_table_create, staging_i94countrycode_table_create, staging_usdp_table_create, staging_usdr_table_create, staging_temper_table_create, staging_alc_table_create, staging_visapost_table_create, staging_alpha2countrycode_table_create, staging_iap_table_create, staging_wc_table_create, staging_USstatecode_table_create, fact_ids_table_create, dim_visatype_table_create, dim_ac_table_create, dim_USPoE_table_create, dim_uszips_table_create, dim_i94countrycode_table_create, dim_usdp_table_create, dim_usdr_table_create, dim_temper_table_create, dim_alc_table_create, dim_visapost_table_create, dim_alpha2countrycode_table_create, dim_iap_table_create, dim_wc_table_create, dim_USstatecode_table_create]

drop_table_queries = [fact_ids_drop, dim_visatype_drop, dim_ac_drop, dim_USPoE_drop, dim_uszips_drop, dim_i94countrycode_drop, dim_usdp_drop, dim_usdr_drop, dim_temper_drop, dim_alc_drop, dim_visapost_drop, dim_alpha2countrycode_drop, dim_iap_drop, dim_wc_drop, dim_USstatecode_drop, staging_ids_drop, staging_visatype_drop, staging_ac_drop, staging_USPoE_drop, staging_uszips_drop, staging_i94countrycode_drop, staging_usdp_drop, staging_usdr_drop, staging_temper_drop, staging_alc_drop, staging_visapost_drop, staging_alpha2countrycode_drop, staging_iap_drop, staging_wc_drop, staging_USstatecode_drop]

insert_table_queries = [dim_visatype_insert, dim_iap_insert, dim_usstatecode_insert, dim_uszips_insert, dim_ac_insert, dim_USPoE_insert, dim_alpha2countrycode_insert, dim_i94countrycode_insert, dim_usdp_insert, dim_usdr_insert, dim_temper_insert, dim_wc_insert, dim_alc_insert, dim_visapost_insert, fact_ids_insert]

