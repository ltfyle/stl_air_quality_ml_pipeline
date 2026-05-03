# Pipeline that extracts historical air quality, transforms and stores it as a csv file
from AQS_etl_tools import CreateDataFrame, TransformCityData
import pandas as pd
import os 

# Setup 
#------------------------------------------------------------------
# api variables 
    # Saint Louis County: 189, 
    # Saint Louis City: 510
    # NO2: 42602, 
    # PM2.5: 88101
county_code=189
city_code=510
AQS_KEY="get key from AQS website"
AQS_EMAIL="email"


# Extracting the data, loading it into dataframes
#-------------------------------------------------------------------
# 1. Initializes the paramaters for the api request:
    # 1.1 city data for NO2, PM25
city_NO2=CreateDataFrame(510,42602,AQS_KEY,AQS_EMAIL)
city_PM25=CreateDataFrame(510,88101,AQS_KEY,AQS_EMAIL)
    # 1.2 county data NO2, PM25
county_NO2=CreateDataFrame(189,42602,AQS_KEY,AQS_EMAIL)
county_PM25=CreateDataFrame(189,88101,AQS_KEY,AQS_EMAIL)

# 2. Pulls the data from the api, transforms into a dataframe, 
    # drops 
        # 'uncertainty',
        # 'state_code',
        # 'county_code','state','county',
        # 'datum',
        # 'date_of_last_change',
        # 'cbsa_code',
        # 'date_gmt','time_gmt',
        # 'units_of_measure_code',
        # 'sample_duration_code',
        # 'method','method_type',
        # 'method_code','detection_limit'

city_NO2=city_NO2.get_air_data()
city_PM25=city_PM25.get_air_data()
county_NO2=county_NO2.get_air_data()
county_PM25=county_PM25.get_air_data()

# create transformed dataframes with mean sample measurement, per site, per day 
# stores in the TransformCityData object 
# -----------------------------------------------------------------------------

# 1. Load the raw dataframes into the TransformCityData objects

city_NO2_Data=TransformCityData(city_NO2)
city_PM25_Data=TransformCityData(city_PM25)
county_NO2_Data=TransformCityData(county_NO2)
county_PM25_Data=TransformCityData(county_PM25)

# 2. actually transofroms the data 
# yeah, this isnt the best design but it was my first attempt at something like this
city_NO2_Data.transform_data()
city_PM25_Data.transform_data()
county_PM25_Data.transform_data()
county_NO2_Data.transform_data()

# 3. the transformed datasets 
df_city_NO2=city_NO2_Data.df_transformed
df_city_PM25=city_PM25_Data.df_transformed
df_county_NO2=county_NO2_Data.df_transformed
df_county_PM25=county_NO2_Data.df_transformed

# Merge, encode variables & Load to csv
#----------------------------------------------------------------
# 1. merge the city and county data for each pollutant
df_NO2=pd.concat([df_city_NO2,df_county_NO2]).sort_index()
df_PM25=pd.concat([df_city_PM25,df_county_PM25]).sort_index()

# 2. replace string variables with numeric encoding
mapping = {'1 HOUR': 1, '24 HOUR': 24}

df_NO2['samp_dur_hrs'] = df_NO2['sample_duration'].map(mapping)
df_NO2.drop('sample_duration', axis=1, inplace=True)
df_PM25['samp_dur_hrs'] = df_PM25['sample_duration'].map(mapping)
df_PM25.drop('sample_duration', axis=1, inplace=True)

# 3. load as csv file
if os.path.isfile("stl_no2_historic.csv"):
    print("File exists")
else:
    df_NO2.to_csv("stl_NO2_historic.csv")

if os.path.isfile("stl_pm25_historic.csv"):
    print("File exists")
else:
    df_PM25.to_csv("stl_pm25_historic.csv")

print("Success! check the directory.")





