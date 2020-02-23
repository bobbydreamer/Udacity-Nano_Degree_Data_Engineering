'''
This program is a copy of process-df.py, created to see if its faster to process all the SAS datasets together. 
This is faster by 500seconds in one run and 1000seconds slower in another. 

New function in this program : process_ids2
'''
import pandas as pd
import os
import glob
#import psycopg2
import pandas as pd
import numpy as np
#from sql_queries import *
import datetime as dt
import json
from fuzzywuzzy import fuzz, process
import configparser
from time import time
import boto3

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, FloatType, TimestampType, LongType, DateType, NullType

import datetime #Required for ts conversion
#from pyspark.sql.functions import udf
#from pyspark.sql import functions as F

#from pyspark.sql.functions import lit
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second

#from pyspark.sql.functions import *
from pyspark.sql.functions import udf, lit, datediff, when, col
###

### Functions
def create_spark_session():
    """
    Summary line. 
    Create spark session
  
    Parameters: None
  
    Returns: 
    spark object
    """            
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .appName("Project Capstone") \
        .enableHiveSupport().getOrCreate()

    return spark


def strip_all_columns(df):
    """
    Summary line.
    Strip all columns in a dataframe
    
    Parameters:
    arg1 (dataframe)
    
    Returns:
    dataframe
    """
    for colu in df.columns:
        if df[colu].dtype == 'object':            
            #print('COL = ',col,' - ',df[col].dtype)
            #df[col] = df[col].str.strip()           
            mask = df[colu].notnull()
            df.loc[mask, colu] = df.loc[mask, colu].map(str.strip)            
            #df[col] = df[col].map(str.strip)
    return df

def difference (list1, list2): 
    """
    Summary line. 
    Difference between two lists used to compare list of two sets of columns

    Parameters: 
    arg1 (list 1)
    arg2 (list 2)

    Returns: 
    list with difference
    
    Sample : print(difference(keep_columns, df_jan.columns))
    """                
    list_dif = [i for i in list1 + list2 if i not in list1 or i not in list2]
    return list_dif

def fuzzymatch_city_get_ratio(row, df_uszips):
    """
    Summary line.
    Match city with US Cities list
    
    Parameters:
    arg1 (dataframe row)
    
    Returns:
    Panda series containing matched city name & score
    """
    city, state_code = row['City'], row['State Code']
    #print('City = ', city, 'State code = ',state_code)
    cities = df_uszips.loc[df_uszips.state_id==state_code, 'city'].str.lower().unique()    
    #print(process.extractOne(city, cities, scorer=fuzz.ratio))
    return pd.Series(process.extractOne(city, cities, scorer=fuzz.ratio))

def to_datetime(x):
    """
    Summary line. 
    Converts SAS date
  
    Parameters: 
    arg1 (days)    
  
    Returns: 
    date object or None
    """        
    try:
        start = dt.datetime(1960, 1, 1).date()
        return start + dt.timedelta(days=int(x))
    except:
        return None

udf_to_datetime_sas = udf(lambda x: to_datetime(x), DateType())


def to_datetimefrstr(x):
    """
    Summary line. 
    Converts date format
  
    Parameters: 
    arg1 (date)
  
    Returns: 
    date object or None
    """            
    try:
        return dt.datetime.strptime(x, '%m%d%Y')
    except:
        return None

udf_to_datetimefrstr = udf(lambda x: to_datetimefrstr(x), DateType())

def cdf_Ymd_to_mmddYYYY(x):
    """
    Summary line. 
    Converts date format
  
    Parameters: 
    arg1 (date)
  
    Returns: 
    date object or None
    """                
    try:
        return dt.datetime.strptime(x, '%Y%m%d')
    except:
        return None

udf_cdf_Ymd_to_mmddYYYY = udf(lambda x: cdf_Ymd_to_mmddYYYY(x), DateType())

def cdf_mdY_to_mmddYYYY(x):
    """
    Summary line. 
    Converts date format
  
    Parameters: 
    arg1 (date)
  
    Returns: 
    date object or None
    """                
    try:
        return dt.datetime.strptime(x, '%m%d%Y')
    except:
        return None

udf_cdf_mdY_to_mmddYYYY = udf(lambda x: cdf_mdY_to_mmddYYYY(x), DateType())


##### Process Functions #####
def process_template(spark, input_data_path):
    """
    Summary line. 
    Process song data
  
    Parameters: 
    arg1 (spark object)
    arg2 (Read input from this path which can be local or S3)
  
    Returns: 
    song_df dataframes
    """    
    
    ps_start = time()
    print('Starting to process song data')


    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process songs file', round(ps_et,2) ))      
    return song_df


def process_i94countrycode(df_i94countrycode1):
    """
    Summary line. 
    Process i94countrycode dataframe
  
    Parameters: 
    arg1 (dataframe)
  
    Returns: 
    Processed dataframe
    """                
    ps_start = time()
    print('{} : Starting to process df_i94countrycode'.format(dt.datetime.now()))
    
    # Update mexico
    temp = ['MEXICO Air Sea, and Not Reported (I-94, no land arrivals)']
    df_i94countrycode1.loc[df_i94countrycode1['country'].isin(temp), 'country']='MEXICO'
    # Below replace was not working
    #df_i94countrycode1['country'] = df_i94countrycode1['country'].str.replace('MEXICO Air Sea, and Not Reported (I-94, no land arrivals)','MEXICO')

    # Remove invalid codes / Collapsed / No Country
    df_i94countrycode1 = df_i94countrycode1[~df_i94countrycode1.country.str.lower().str.contains('invalid')]
    df_i94countrycode1 = df_i94countrycode1[~df_i94countrycode1.country.str.lower().str.contains('no country')]
    df_i94countrycode1 = df_i94countrycode1[~df_i94countrycode1.country.str.lower().str.contains('collapsed')]
    
    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process df_i94countrycode', round(ps_et,2) ))      
    
    return df_i94countrycode1


def process_df_USPoE(df_USPoE1):
    """
    Summary line. 
    Process df_USPoE1 dataframe
  
    Parameters: 
    arg1 (dataframe)
  
    Returns: 
    Processed dataframe
    """                    
    ps_start = time()
    print('{} : Starting to process df_USPoE'.format(dt.datetime.now()))    
    
    # df_USPoE : Split one column into two
    df_USPoE1["citystate"] = df_USPoE1["citystate"].map(str.strip)

    df_USPoE1[['city', 'state']] = df_USPoE1['citystate'].str.rsplit(",",n=1, expand=True)
    df_USPoE1 = strip_all_columns(df_USPoE1)
    # trim all columns
    #for col in df_USPoE1.columns:
    #    df_USPoE1[col] = df_USPoE1[col].str.strip()

    # df_USPoE : Correct incorrect state code values
    df_USPoE1 = strip_all_columns(df_USPoE1).copy()
    #df_USPoE2['state'] = df_USPoE2['state'].str.replace('\(BPS\)', '')
    df_USPoE1['state'] = df_USPoE1.state.str.replace(r'\(.*\)$', '')
    df_USPoE1['state'] = df_USPoE1['state'].str.replace('#ARPT', '')
    df_USPoE1['state'] = df_USPoE1['state'].str.replace('#INTL', '')
    df_USPoE1['state'] = df_USPoE1['state'].str.replace('WASHINGTON', 'WA')
    df_USPoE1['state'] = df_USPoE1['state'].str.replace('MX', 'MEXICO')

    df_USPoE1['city'] = df_USPoE1['city'].str.replace('#ARPT', '')
    df_USPoE1['city'] = df_USPoE1['city'].str.replace('-ARPT', '')
    df_USPoE1['city'] = df_USPoE1['city'].str.replace('ARPT', '')
    df_USPoE1['city'] = df_USPoE1['city'].str.replace('INTL', '')
    df_USPoE1 = strip_all_columns(df_USPoE1).copy()

    # df_USPoE : Remove invalid rows & ports outside US
    df_USPoE1 = strip_all_columns(df_USPoE1).copy()
    df_USPoE1 = df_USPoE1[df_USPoE1.state.notnull()]

    cond1 = df_USPoE1.city.str.lower().str.contains('collapsed')
    df_USPoE1 = df_USPoE1[~cond1]

    cond1 = df_USPoE1.city.str.lower().str.contains('no port')
    df_USPoE1 = df_USPoE1[~cond1]

    cond1 = df_USPoE1.city.str.lower().str.contains('unknown')
    df_USPoE1 = df_USPoE1[~cond1]

    cond1 = df_USPoE1.city.str.lower().str.contains('identifi')
    df_USPoE1 = df_USPoE1[~cond1]

    df_USPoE1 = df_USPoE1[df_USPoE1.state.str.len() == 2]

    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process df_USPoE', round(ps_et,2) ))      

    
    return df_USPoE1


def process_df_ac(df_ac1, df_iap):
    """
    Summary line. 
    Process df_ac dataframe
  
    Parameters: 
    arg1 (dataframe)
    arg2 (dataframe)
  
    Returns: 
    Processed dataframe
    """                        
    ps_start = time()
    print('{} : Starting to process df_ac'.format(dt.datetime.now()))
    #print('== df_ac1 = ',df_ac1.shape[0])
    
    # df_ac : Keeping only US Airports which serves passenger flights and has customs & immigration
    # Dropping unused columns gps_code, ident
    drop_cols = ['gps_code', 'ident']
    df_ac1.drop(drop_cols, inplace = True, axis = 1)

    #Keep only US Airports
    df_ac1 = df_ac1[df_ac1.iso_country =='US'].copy()

    # Correcting wrong continent value 
    cond1 = df_ac1.iso_country =='US'
    cond4 = df_ac1['continent'].isnull()
    ix = df_ac1[cond1 & ~cond4].index.values[0]
    df_ac1.at[ix,'continent']=np.nan

    # Eliminate rows which has iata code as null
    df_ac1 = df_ac1[~df_ac1.iata_code.isnull()].copy()

    # Keep only airport which is of type small_airport, medium_airport & large_airport
    temp = ['small_airport', 'medium_airport', 'large_airport']
    df_ac1 = df_ac1[df_ac1['type'].isin(temp)].copy()

    # Split geo-coordinates into separate columns
    df_ac1[['longitude','latitude']] = df_ac1.coordinates.str.split(", ",expand=True)

    # Extract state code from iso_region
    df_ac1['state'] = df_ac1.iso_region.str.slice(start=3)

    # Add new column facilities to indicate whether airport has Customs & Immigration
    for code in df_iap['IATA']:
        #print(code)    
        df_ac1.loc[df_ac1['iata_code'] == code, 'facilities'] = 'CI'

    #Keeping only airports which has Customs & Immigration
    df_ac1 = df_ac1[df_ac1.facilities.notnull()].copy()

    # Removing rows having 'Duplicate' in airport names
    df_ac1 = df_ac1[~df_ac1.name.str.contains('uplicate')]

    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process df_ac', round(ps_et,2) ))      

    #print('== df_ac1 = ',df_ac1.shape[0])    
    return df_ac1


def process_ids2(spark, sas_raw_datasets, df_i94countrycode1, df_USPoE1, df_ac1, df_visatype1):
    """
    Summary line. 
    Process i94 dataframe
  
    Parameters: 
    arg1 (dataframe)
    arg2 (dataframe)
    arg3 (dataframe)
    arg4 (dataframe)
    arg5 (dataframe)
    arg6 (dataframe)
  
    Returns: 
    Processed dataframe
    """                    

    ps_start = time()
    print('{} : Starting to process dfs_ids'.format(dt.datetime.now()))
    #print('== df_i94countrycode1 = ',df_i94countrycode1.shape[0])
    #print('== df_USPoE1 = ',df_USPoE1.shape[0])
    #print('== df_ac1 = ',df_ac1.shape[0])
    #print('== df_visatype1 = ',df_visatype1.shape[0])
    
    dfs_i94countrycode1 = spark.createDataFrame(df_i94countrycode1)
    dfs_USPoE1 = spark.createDataFrame(df_USPoE1)

    ac_schema = StructType([
        StructField("type", StringType()),
        StructField("name", StringType()),
        StructField("elevation_ft", FloatType()),
        StructField("continent", StringType()),
        StructField("iso_country", StringType()),
        StructField("iso_region", StringType()),
        StructField("muncipality", StringType()),
        StructField("iata_code", StringType()),
        StructField("local_code", StringType()),
        StructField("coordinates", StringType()),   
        StructField("longitude", StringType()),   
        StructField("latitude", StringType()),   
        StructField("state", StringType()),   
        StructField("facilities", StringType()),   
    ])
    dfs_ac1 = spark.createDataFrame(df_ac1, ac_schema)

    
    ###
    flag=True
    for sasd in sas_raw_datasets:
        print(' ==> {} : Processing sas dataset {}'.format(dt.datetime.now(), sasd))
        temp_i94 = spark.read.format('com.github.saurfang.sas.spark').load(sasd)   
        assert temp_i94.count() != 0,"dfs_ids1 is empty"   

        # matflag is null
        temp_i94 = temp_i94.filter(temp_i94.matflag.isNotNull())

        # visatype GMT  
        temp = df_visatype1.visatype.tolist()
        temp_i94 = temp_i94.filter( temp_i94.visatype.isin(temp) )

        # i94mode other than 1 2 3
        temp = [1, 2, 3]
        temp_i94 = temp_i94.filter( temp_i94.i94mode.isin(temp) )

        # gender is null
        temp_i94 = temp_i94.filter(temp_i94.gender.isNotNull())

        # Remove rows having invalid CoC & CoR
        temp = df_i94countrycode1.code.astype('int').tolist()
        temp_i94 = temp_i94.filter( temp_i94.i94cit.isin(temp) )
        temp_i94 = temp_i94.filter( temp_i94.i94res.isin(temp) )

        # Remove Non-US Port of entry data
        df1 = dfs_USPoE1.select('code')
        df2 = temp_i94.select('i94port')
        temp = df2.subtract(df1) #.collect()
        tempArr = [row.i94port for row in temp.collect()]
        temp_i94 = temp_i94.filter(~temp_i94.i94port.isin(tempArr))

        # Dropping unused columns
        # Only below columns are used for analysis
        keep_columns = ['i94cit', 'i94res', 'i94port', 'arrdate', 'i94mode', 'i94addr', 'depdate'
                        , 'i94bir', 'i94visa', 'dtadfile', 'visapost', 'occup', 'biryear'
                        , 'dtaddto', 'gender', 'airline', 'admnum', 'fltno', 'visatype']
        drop_cols = difference(keep_columns, temp_i94.columns)
        #print(drop_cols)
        temp_i94 = temp_i94.drop(*drop_cols)    
                
        # Concatenating i94 spark dataframes
        if flag:
            dfs_ids1 = temp_i94
            flag=False
        else:
            dfs_ids1 = dfs_ids1.union(temp_i94)
    ###
    
    
    # This takes 2 mins due to count() without it takes 3.36s
    # Delete Port of Entries which lacks International Passenger Handling Facility operated by US Customs.
    # df_ac has to be processed after df_USPoE1 and before df_ids1
    temp = dfs_ac1.filter( dfs_ac1.facilities.isNotNull() ).select('state').dropDuplicates()
    df1 = [row.state for row in temp.collect()]
    #print('Total US States that has Customs & Immigration : ',len(df1))
    temp = dfs_USPoE1.filter(~dfs_USPoE1.state.isin(df1)).select('code').dropDuplicates()
    df2 = [row.code for row in temp.collect()]
    #print('Number of Port of Entries that needs to be deleted from df_USPoE : ',len(df2))
    #print('Total rows that will be deleted from df_ids due to PoE that doesnt have Customs & Immigration : '
    #      ,dfs_ids1.filter( dfs_ids1.i94port.isin(df2)).count())
    #print('Before : dfs_ids1 = ',dfs_ids1.count())
    dfs_ids1 = dfs_ids1.filter(~dfs_ids1.i94port.isin(df2))
    #print('After  : dfs_ids1 = ',dfs_ids1.count())

    # Convert floats to ints
    cols_to_convert_float_to_integer = ['i94cit', 'i94res', 'arrdate', 'i94mode', 'depdate', 'i94bir'
                                        , 'i94visa', 'biryear', 'admnum']
    for colu in cols_to_convert_float_to_integer:    
        dfs_ids1 = dfs_ids1.na.fill(0, subset=[colu])
        dfs_ids1 = dfs_ids1.withColumn(colu, dfs_ids1[colu].cast(IntegerType()))

    # Mapping : Codes to descriptive
    #temp = {'1' : 'Air', '2' : 'Sea', '3' : 'Land', '9' : 'Not reported'}
    temp = [["1", "Air"], ["2", "Sea"], ["3","Land"], ["9", "Not reported"]]
    i94mode = spark.sparkContext.parallelize(temp).toDF(["code", "arrival_mode"])
    dfs_ids1 = dfs_ids1.join(i94mode, dfs_ids1.i94mode == i94mode.code).select(dfs_ids1["*"], i94mode["arrival_mode"])

    temp = [["1", "Business"], ["2", "Pleasure"], ["3", "Student"]]
    i94visa = spark.sparkContext.parallelize(temp).toDF(["code", "visit_purpose"])
    dfs_ids1 = dfs_ids1.join(i94visa, dfs_ids1.i94visa == i94visa.code).select(dfs_ids1["*"], i94visa["visit_purpose"])
    ##
    #from pyspark.sql.functions import *
    # Conversion of SAS encoded dates(arrdate & depdate)
    dfs_ids1 = dfs_ids1.withColumn("arrival_dt", udf_to_datetime_sas(dfs_ids1.arrdate))
    dfs_ids1 = dfs_ids1.withColumn("departure_dt", udf_to_datetime_sas(dfs_ids1.depdate))

    dfs_ids1 = dfs_ids1.withColumn("DaysinUS", datediff("departure_dt", "arrival_dt"))

    dfs_ids1 = dfs_ids1.withColumn("added_to_i94", udf_cdf_Ymd_to_mmddYYYY(dfs_ids1.dtadfile))
    dfs_ids1 = dfs_ids1.withColumn("allowed_until", udf_cdf_mdY_to_mmddYYYY(dfs_ids1.dtaddto))

    # Below corrections are carried out due to above adding 1960-01-01
    dfs_ids1 = dfs_ids1.withColumn("arrival_dt",when(col("arrival_dt")=="1960-01-01",lit(None)).otherwise(col("arrival_dt")))
    dfs_ids1 = dfs_ids1.withColumn("departure_dt",when(col("departure_dt")=="1960-01-01",lit(None)).otherwise(col("departure_dt")))
    dfs_ids1 = dfs_ids1.withColumn("DaysinUS",when(col("arrival_dt").isNull(),lit(None)).otherwise(col("DaysinUS")))
    dfs_ids1 = dfs_ids1.withColumn("DaysinUS",when(col("departure_dt").isNull(),lit(None)).otherwise(col("DaysinUS")))

    # Departure date can't before Arrival date 
    # ~(arrival date > departure date ) or (departure date can be null)
    dfs_ids1 = dfs_ids1.filter(~(dfs_ids1.arrival_dt > dfs_ids1.departure_dt) | (dfs_ids1.departure_dt.isNull()))

    # Change date format to YYYY-mm-dd
    dfs_ids1 = dfs_ids1.withColumn("added_to_i94", udf_cdf_Ymd_to_mmddYYYY(dfs_ids1.dtadfile))
    dfs_ids1 = dfs_ids1.withColumn("allowed_until", udf_cdf_mdY_to_mmddYYYY(dfs_ids1.dtaddto))

    # Columns Rename
    dfs_ids1 = (dfs_ids1
                .withColumnRenamed("i94bir",  "age")
                .withColumnRenamed("i94cit", "CoC")
                .withColumnRenamed("i94res", "CoR")
                .withColumnRenamed("i94port", "PoE")
                .withColumnRenamed("i94addr", "landing_state")
                .withColumnRenamed("visapost", "visa_issued_in"))

    # Entry_Exit
    dfs_ids1 = dfs_ids1.withColumn("entry_exit",when(col("departure_dt").isNull(),lit("entry")).otherwise(lit("exit")))

    # Gender X to O
    dfs_ids1 = dfs_ids1.withColumn("gender", when(col("gender")=="X", lit("O")).otherwise(col("gender")))

    # Final Drop of unused columns
    drop_cols = ['i94mode', 'i94visa', 'arrdate', 'depdate', 'dtadfile', 'dtaddto']
    #print(drop_cols)
    dfs_ids1 = dfs_ids1.drop(*drop_cols)

    # Adding month which is used when saving file in parquet format partioning by month & landing state
    dfs_ids1 = dfs_ids1.withColumn("month", month("arrival_dt"))

    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process dfs_ids', round(ps_et,2) ))    
    return dfs_ids1

def process_df_visatype(df_visatype1):
    """
    Summary line. 
    Process df_visatype dataframe
  
    Parameters: 
    arg1 (dataframe)
  
    Returns: 
    Processed dataframe
    """                        
    
    ps_start = time()
    print('{} : Starting to process df_visatype'.format(dt.datetime.now()))
    
    df_visatype1["visatype"] = df_visatype1["visatype"].map(str.strip)        
    
    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process df_visatype', round(ps_et,2) ))      
    
    return df_visatype1

def process_df_uszips(df_uszips1):
    """
    Summary line. 
    Process df_uszips dataframe
  
    Parameters: 
    arg1 (dataframe)
  
    Returns: 
    Processed dataframe
    """                        
    
    ps_start = time()
    print('{} : Starting to process df_uszips'.format(dt.datetime.now()))
    
    drop_cols = ['zcta', 'parent_zcta', 'all_county_weights', 'imprecise', 'military', 'timezone']
    df_uszips1.drop(drop_cols, inplace = True, axis = 1)        
    
    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process df_uszips', round(ps_et,2) ))      

    return df_uszips1

def process_df_uscd(df_uscd1, df_uszips1):
    """
    Summary line. 
    Process df_uscd dataframe
  
    Parameters: 
    arg1 (dataframe)
    arg2 (dataframe)
  
    Returns: 
    Processed dataframes (df_uscd1, df_usdp, df_usdr)
    """                        
    
    ps_start = time()
    print('{} : Starting to process df_uscd'.format(dt.datetime.now()))
    
    # Convert floats to ints
    cols_to_convert_float_to_int = ['Male Population', 'Female Population', 'Number of Veterans', 'Foreign-born', 'Total Population', 'Count']

    for col in cols_to_convert_float_to_int:
        df_uscd1[col] = df_uscd1[col].replace(np.nan, 0)
        df_uscd1[col] = df_uscd1[col].astype(int)

    # Keeping only rows where MP + FP = TP 
    df_uscd1 = df_uscd1[(df_uscd1['Male Population'] + df_uscd1['Female Population'] == df_uscd1['Total Population'])].copy()

    # US : Cities check to if any names are misspelled
    df1 = df_uszips1[['city']]
    lst = df1.city.str.lower().unique()
    test = df_uscd1[~df_uscd1.City.str.lower().isin(lst)]

    # removing rows of misspelled cities from df_uscd1 to process separately
    df_uscd1 = df_uscd1[df_uscd1.City.str.lower().isin(lst)]

    test1 = test.copy()

    # Performing fuzzy match against the city rows which were not found in df_uszips
    #test2 = test1.apply(fuzzymatch_city_get_ratio, axis=1)
    test2 = test1.apply(lambda x: fuzzymatch_city_get_ratio(x, df_uszips1), axis=1)
    #df['NewCol'] = df.apply(lambda x: segmentMatch(x['TimeCol'], x['ResponseCol']), axis=1)

    test2.columns = ['new_city_name','score']
    test1 = test1.join(test2)

    # Updating city names with score >= 80
    test1['City'] = test1.apply(lambda x: x['new_city_name'] if x.score >= 80 else x['City'], axis=1)

    # Convert lowercase city names to title case as before
    test1.City = test1.City.str.strip().str.title()

    # Dropping columns in fuzzy match dataframes
    drop_cols = ['new_city_name', 'score']
    test1.drop(drop_cols, inplace = True, axis = 1)

    # Concatinating all the dataframes 1
    test3 = [df_uscd1, test1]
    df_uscd1 = pd.concat(test3).copy()

    # Merging duplicates by avging & summing
    temp = df_uscd1[df_uscd1.City=='Los Angeles'].copy()
    df_uscd1 = df_uscd1[df_uscd1.City!='Los Angeles'].copy()
    temp = temp.groupby(['City','State', 'State Code', 'Race']).agg({'Median Age':'mean','Male Population':'sum', 'Female Population': 'sum'
                                                                        , 'Number of Veterans':'sum', 'Foreign-born':'sum'
                                                                        , 'Average Household Size':'mean', 'Count':'sum' }).reset_index().copy()
    # Concatinating all the dataframes 2
    frames = [df_uscd1, temp]
    # Encountered this warning while doing pd.concat : FutureWarning: Sorting because non-concatenation axis is not aligned. A future version of pandas will change to not sort by default.
    df_uscd1 = pd.concat(frames, sort=False).copy()

    # Above concat process changes the total population to float, so converting it back to Int
    df_uscd1['Total Population'] = df_uscd1['Total Population'].replace(np.nan, 0)
    df_uscd1['Total Population'] = df_uscd1['Total Population'].astype(int)

    # Split based on Gender
    df_usdp = df_uscd1[['State', 'City', 'Median Age', 'State Code', 'Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born']]
    df_usdp = df_usdp.drop_duplicates()

    # Split based on Race
    df_usdr = df_uscd1[['State', 'City', 'State Code', 'Race', 'Count']].copy()

    # Summing up the duplicates(ie., East Los Angeles -> Los Angeles)
    df_usdr = df_usdr.groupby(['State', 'City', 'State Code', 'Race']).sum().reset_index().copy()

    # Unmelting to converts Race rows to columns
    df_usdr = df_usdr.set_index(['State','City','State Code', 'Race']).Count.unstack().reset_index()
    df_usdr.columns.name = None
    df_usdr[df_usdr.State =='Texas'].head()

    # Convert floats to ints
    cols_to_convert_float_to_int = ['American Indian and Alaska Native','Asian', 'Black or African-American', 'Hispanic or Latino', 'White']

    for col in cols_to_convert_float_to_int:
        df_usdr[col] = df_usdr[col].replace(np.nan, 0)
        df_usdr[col] = df_usdr[col].astype(int)

    # Rename column names
    df_usdr.rename(columns={"State Code":"State_Code", "American Indian and Alaska Native": "American_Indian_and_Alaska_Native"
                            , "Black or African-American":"Black_or_African_American"
                            , "Hispanic or Latino":"Hispanic_or_Latino"}, inplace=True)

    df_usdp.rename(columns={ "Median Age": "Median_Age" , "State Code":"State_Code", "Male_Population":"Male_Population"
                            , "Female Population":"Female_Population", "Total Population":"Total_Population" 
                            , "Number of Veterans":"Number_of_Veterans", "Foreign-born" : "Foreign_born" }, inplace=True)

    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process df_uscd', round(ps_et,2) ))      
    
    return df_uscd1, df_usdp, df_usdr


def process_df_temper(df_temper1):
    """
    Summary line. 
    Process df_temper dataframe
  
    Parameters: 
    arg1 (dataframe)
  
    Returns: 
    Processed dataframe
    """                        

    ps_start = time()
    print('{} : Starting to process df_temper'.format(dt.datetime.now()))
    
    # Keeping only USA
    df_temper1 = df_temper1[df_temper1.Country=='United States'].copy()

    # Rounding temperature to decimals=3 
    df_temper1['AverageTemperature'] = df_temper1['AverageTemperature'].round(decimals=3)

    # Dropping unused columns
    drop_cols = ['AverageTemperatureUncertainty', 'Latitude', 'Longitude']
    df_temper1.drop(drop_cols, inplace = True, axis = 1)

    # Removing missing temperatures
    df_temper1 = df_temper1[~(df_temper1.AverageTemperature.isnull())]

    # Eliminating the duplicates(ie., multiple locations in same city)
    df_temper1 = df_temper1.drop_duplicates(['dt', 'City', 'Country'],keep= 'first')

    # Convert dt to datetime from object
    df_temper1['dt'] = pd.to_datetime(df_temper1.dt)

    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process df_temper', round(ps_et,2) ))
    
    return df_temper1


def process_df_wc(df_wc1):
    """
    Summary line. 
    Process df_wc dataframe
  
    Parameters: 
    arg1 (dataframe)
  
    Returns: 
    Processed dataframe
    """
    
    ps_start = time()
    print('{} : Starting to process df_wc'.format(dt.datetime.now()))
    
    df_wc1['population'] = df_wc1['population'].replace(np.nan, 0)
    df_wc1['population'] = df_wc1['population'].astype(int)        
    
    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process df_wc', round(ps_et,2) ))    
    return df_wc1
    

def main():   
    total_start = time()
    spark = create_spark_session()

    ##### Read datasets ##### 
    print('{} : Starting to read datasets'.format(dt.datetime.now()))
    # JSON to dictionary
    fname = 'inputs/i94cit.json'
    i94cit = json.load(open(fname))

    fname = 'inputs/i94port.json'
    i94port = json.load(open(fname))

    # Airport Code Table
    df_ac = pd.read_csv('inputs/airport-codes_csv.csv')

    # Convert JSON Dictionaries to Dataframes
    df_i94countrycode = pd.DataFrame(list(i94cit.items()), columns=['code', 'country'])

    df_USPoE = pd.DataFrame(list(i94port.items()), columns=['code', 'citystate'])

    # International Airports
    df_iap = pd.read_csv('inputs/InternationalAirports.csv', sep=',', encoding = "ISO-8859-1")

    # Visa Type
    df_visatype = pd.read_csv('inputs/visatype2.csv', sep='|')

    # U.S. City Demographic Data
    df_uscd = pd.read_csv('inputs/us-cities-demographics.csv', sep=';')

    # World Temperature Data
    df_temper = pd.read_csv('inputs/world_temperature.csv')

    # Airliner Codes - Can be used join airline column in i94-immigration data
    df_alc = pd.read_csv('inputs/airline-codes.csv')

    # Can be used to map to df_ids.visapost column
    df_visapost = pd.read_csv('inputs/visapost.csv')

    # US zip-codes
    df_uszips = pd.read_csv('inputs/uszips.csv')

    # Country Codes
    df_alpha2countrycode = pd.read_csv('inputs/country-codes2.csv', sep='|')

    # World Cities
    df_wc = pd.read_csv('inputs/worldcities.csv', sep=',')

    # US States
    df_USstatecode = pd.read_csv('inputs/us-states.csv', sep='|')
    
    ##### Create copies of dataframe #####
    df_visatype1 = df_visatype.copy()
    df_ac1 = df_ac.copy()
    df_USPoE1 = df_USPoE.copy()
    df_uszips1 = df_uszips.copy()
    df_i94countrycode1 = df_i94countrycode.copy()
    df_uscd1 = df_uscd.copy()
    df_temper1 = df_temper.copy()
    df_wc1 = df_wc.copy()

    # Running strip() on all string columns
    df_i94countrycode1 =strip_all_columns(df_i94countrycode1).copy()
    df_USPoE1 =strip_all_columns(df_USPoE1).copy()
    df_temper1 = strip_all_columns(df_temper1).copy()
    df_visatype1 = strip_all_columns(df_visatype1).copy()
    df_wc1 = strip_all_columns(df_wc1).copy()

    ##### Process dataframes ##### 
    df_i94countrycode1 = process_i94countrycode(df_i94countrycode1)    
    assert df_i94countrycode1.shape[0] != 0,"df_i94countrycode1 is empty"
    
    df_USPoE1 = process_df_USPoE(df_USPoE1)
    assert df_USPoE1.shape[0] != 0,"df_USPoE1 is empty"

    df_ac1 = process_df_ac(df_ac1, df_iap)
    assert df_ac1.shape[0] != 0,"df_ac1 is empty"
    assert df_iap.shape[0] != 0,"df_iap is empty"
    
    df_visatype1 = process_df_visatype(df_visatype1)
    assert df_visatype1.shape[0] != 0,"df_visatype1 is empty"

    ###
    print('Starting spark read of SAS datasets')
    data_dir = "../../data/18-83510-I94-Data-2016/"
    files = os.listdir(data_dir)
    sas_raw_datasets = [data_dir + files[i] for i in range(len(files))]   
    dfs_ids1 = process_ids2(spark, sas_raw_datasets, df_i94countrycode1, df_USPoE1, df_ac1, df_visatype1)
    assert dfs_ids1.count() != 0,"dfs_ids1 is empty"
    ###
    
    #dfs_ids1 =spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    #assert dfs_ids1.count() != 0,"dfs_ids1 is empty"
    #dfs_ids1 = process_ids(spark, dfs_ids1, df_i94countrycode1, df_USPoE1, df_ac1, df_visatype1)
    #assert dfs_ids1.count() != 0,"dfs_ids1 is empty"
        
    df_uszips1 = process_df_uszips(df_uszips1)
    assert df_uszips1.shape[0] != 0,"df_uszips1 is empty"
    
    df_uscd1, df_usdp, df_usdr = process_df_uscd(df_uscd1, df_uszips1)    
    assert df_uscd1.shape[0] != 0,"df_uscd1 is empty"
    assert df_uszips1.shape[0] != 0,"df_uszips1 is empty"
    assert df_usdp.shape[0] != 0,"df_usdp is empty"
    assert df_usdr.shape[0] != 0,"df_usdr is empty"

    df_temper1 = process_df_temper(df_temper1)
    assert df_temper1.shape[0] != 0,"df_temper1 is empty"

    df_wc1 = process_df_wc(df_wc1)
    assert df_wc1.shape[0] != 0,"df_wc1 is empty"
    
    ##### Rows Count ##### 
    print("{} : Cleaned dataframes rows count".format(dt.datetime.now()))
    print("{:15} = {}".format("df_visatype1", df_visatype1.shape[0]))
    print("{:15} = {}".format("df_ac1", df_ac1.shape[0]))
    print("{:15} = {}".format("df_USPoE1", df_USPoE1.shape[0]))
    print("{:15} = {}".format("df_uszips1", df_uszips1.shape[0]))
    
    #Full dataframe is 28217609(process-df.py)
    print("{:15} = {}".format("dfs_ids1", dfs_ids1.count()))    
    
    print("{:15} = {}".format("df_i94countrycode1", df_i94countrycode1.shape[0]))
    print("{:15} = {}".format("df_uscd1", df_uscd1.shape[0]))
    print("{:15} = {}".format("df_usdp", df_usdp.shape[0]))
    print("{:15} = {}".format("df_usdr", df_usdr.shape[0]))
    print("{:15} = {}".format("df_temper1", df_temper1.shape[0]))
    print("{:15} = {}".format("df_wc1", df_wc1.shape[0]))

    print("{} : Datasets with no changes/updates".format(dt.datetime.now()))
    print("{:15} = {}".format("df_alc", df_alc.shape[0]))
    print("{:15} = {}".format("df_visapost", df_visapost.shape[0]))
    print("{:15} = {}".format("df_alpha2countrycode", df_alpha2countrycode.shape[0]))
    print("{:15} = {}".format("df_iap", df_iap.shape[0]))
    print("{:15} = {}".format("df_USstatecode", df_USstatecode.shape[0]))    
    
    ##### Dataframe writes #####     
    # Cleaned datasets
    """
    print("{} : Write non-i94 dataframes(CSV)".format(dt.datetime.now()))
    
    # Create folder if it doesn't exist
    folder = "./outputs/"
    os.makedirs(os.path.dirname(folder), exist_ok=True)    
    
    df_visatype1.to_csv('outputs/df_visatype1.csv', encoding='utf-8', index=False)
    df_ac1.to_csv('outputs/df_ac1.csv', encoding='utf-8', index=False)
    df_USPoE1.to_csv('outputs/df_USPoE1.csv', encoding='utf-8', index=False)
    df_uszips1.to_csv('outputs/df_uszips1.csv', encoding='utf-8', index=False)
    df_i94countrycode1.to_csv('outputs/df_i94countrycode1.csv', encoding='utf-8', index=False)
    df_uscd1.to_csv('outputs/df_uscd1.csv', encoding='utf-8', index=False)
    df_usdp.to_csv('outputs/df_usdp.csv', encoding='utf-8', index=False)
    df_usdr.to_csv('outputs/df_usdr.csv', encoding='utf-8', index=False)
    df_temper1.to_csv('outputs/df_temper1.csv', encoding='utf-8', index=False)
    df_wc1.to_csv('outputs/df_wc1.csv', encoding='utf-8', index=False)

    # Below datasets have not changes/updates, just writing to another folder
    df_alc.to_csv('outputs/df_alc.csv', encoding='utf-8', index=False)
    df_visapost.to_csv('outputs/df_visapost.csv', encoding='utf-8', index=False)
    df_alpha2countrycode.to_csv('outputs/df_alpha2countrycode.csv', encoding='utf-8', index=False)
    df_iap.to_csv('outputs/df_iap.csv', encoding='utf-8', index=False)
    df_USstatecode.to_csv('outputs/df_USstatecode.csv', encoding='utf-8', index=False)
    """
    
    print("{} : Write i94 dataframes(gzip)".format(dt.datetime.now()))
    dfs_ids1.write\
      .format("com.databricks.spark.csv")\
      .option("header","true")\
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")\
      .save('./outputs-gzip2/dfs_ids1.gzip')    
    
    total_et = time() - total_start
    print("=== {} Total Elapsed time is {} sec\n".format('Main()', round(total_et,2) ))
    print('{} : Done!'.format(dt.datetime.now()))


if __name__ == "__main__":
    main()