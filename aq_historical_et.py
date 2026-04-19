import pandas as pd
pd.set_option('display.max_columns', None)
import time
import requests
# all the methods needed to extract data, drop initial columns, and build dictionaries
## data dict class
class CreateDataDict:

    def __init__(self,df):
        """Collects all methods for creating a data dictionary"""
        self.df=df
        self.data_dict= pd.DataFrame({
        "column": self.df.columns,
        "dtype" : self.df.dtypes.astype(str).values,
        "missingness" : self.df.isna().mean()*100,
        "description": "",
        "notes" : ""
        })
        #self.to_drop=['uncertainty','state_code','county_code','state','county','datum','date_of_last_change','cbsa_code','date_gmt','time_gmt','units_of_measure_code','sample_duration_code','method','method_type','method_code','detection_limit']
       
    def get_data_dict(self):
        return self.data_dict
        
    def _check_initialized(self):
        if self.data_dict is None:
            raise ValueError("Data dictionary not created yet. Run set_self.data_dict() first.")
            

    def update_description(self,column,description):
        """Updates the description column"""
        self._check_initialized()

        mask=self.data_dict["column"]==column

        if not mask.any():
            print("column not found")
            return
        self.data_dict.loc[mask,"description"]=description
        print(self.data_dict.loc[mask])
        
    def update_notes(self,column,note):
        """Updates the notes section"""
        self._check_initialized()

        mask=self.data_dict["column"]==column
        if not mask.any():
            print("column not found")
            return
        self.data_dict.loc[mask,"notes"]=note
        print(self.data_dict.loc[mask])
    def remove_column(self, column):
        """Removes a column row from the data dictionary only"""
        self._check_initialized()

        self.data_dict = self.data_dict[self.data_dict["column"] != column]
    def populate_dictionary(self):
        """populates the data dictionaries"""
        self._check_initialized()
    #    self.update_description('state_code','FIPS code for state')
    #    self.update_description('county_code','FIPS code for county')
        self.update_description('site_number', 'Monitoring Station ID')
        self.update_description('parameter_code','AQS pollutant code') 
        self.update_description('poc','parameter occorence code')
        self.update_description('latitude','geographic area')
        self.update_description('longitude','geographic area')
    #    self.update_description('datum','geodectic reference system for lat log')
        self.update_description('parameter','pollutant name')
        self.update_description('sample_duration','1hr,8hr,24hr period')
    #    self.update_description('sample_duration_code','1:1hr,5:8hr,7:24hr')
        self.update_description('sample_duration_type','HOURLY:1hr avg, DAILY:24hr avg, 8hr RUN AVG:8hr rolling avg')
    #    self.update_description('pollutant_standard','EPA regulatory framework defining acceptable threshold')
        self.update_description('date_local','date collected')
        self.update_description('units_of_measure','units to measure concentration')
        self.update_description('event_type','flagging whether data point effected by exceptional environmental event')
        self.update_description('observation_count','# of observations used to calculate reported summary value')
        self.update_description('observation_percent','how complete the data is for that time period')
        self.update_notes('observation_percent','filter out below 75%')
        self.update_description('validity_indicator','passed AQ checks')
        self.update_description('arithmetic_mean','concentration over 24 hr period')
        self.update_description('first_max_value','peak exposure concentration')
        self.update_notes('first_max_value','best choice for aqi prediction')
        self.update_description('first_max_hour','when peak exposure occored')
        self.update_description('time_local','Time sample was recorded')
        self.update_description('sample_measurement','concentration recorded')
        self.update_description('units_of_measure','units of measure')
    #    self.update_description('units_of_measure_code','code corresponding to units of measure')
        self.update_description('sample_frequency','how often measured')
        self.update_notes('sample_frequency','HOURLY, every 6th day')
    #    self.update_description('detection_limit','concentration detected at')
        self.update_description('qualifier','code for context info')
        self.update_notes('qualifier','some will need replacement with mean')
    #    self.update_description('method_type','FEM:automated,FRM:laboratory')
    #    self.update_description('method','detailed info on method')
    #    self.update_description('method_code','method code')
    #    self.update_description('state','state')
    #    self.update_description('county','county')
    #    self.update_description('date_of_last_change','date added to database')
    #    self.update_description('cbsa_code','Core base statistical area code')
    #    self.update_description('uncertainty','Confidence Level')
        self.update_notes('poc','code for measurements at same site, location and time')
        return self.data_dict
    


## Class to get dataframe

class CreateDataFrame:
    
    def __init__(self,county_code,pollutant_code,key,email):
        """gets the response from aqs.epa.gov/data/api/
        returns a dataframe"""
        self.pollutant_code=pollutant_code
        self.to_drop=['uncertainty','state_code','county_code','state','county','datum','date_of_last_change','cbsa_code','date_gmt','time_gmt','units_of_measure_code','sample_duration_code','method','method_type','method_code','detection_limit']

        self.key=key
        self.email=email
        self.county_code=county_code

    def __get_response(self):
        """returns a response in json format for given parameters *up to five
        and given county code"""
        url_hist_air_quality="https://aqs.epa.gov/data/api/sampleData/byCounty?"
        KEY=self.key
        EMAIL=self.email

        params={
            'email': EMAIL,
            'key' : KEY,
            'param': self.pollutant_code,
            'bdate' : '20250101',
            'edate' : '20251231',
            'state' : '29',
            'county' : self.county_code
        }

        response=requests.get(url_hist_air_quality,params=params)
        if response.status_code==200:
            print("Success")
            return response.json()
        else:
            print(response.text)

    def get_air_data(self):
    
        # responses for given county code, for params
        response=self.__get_response()

        # convert the json into pandas dataframe
        df_out = pd.json_normalize(response, record_path=['Data']) 

        return df_out.drop(columns=self.to_drop)

class TransformCityData:
    def __init__(self,df_city_pollutant):
        self.df_city_pollutant = df_city_pollutant
        self.df_transformed = None

    def transform_data(self):
        """creates a dataframe for pollutant, with mean sample measurement per site, per day stored as class object"""
        df_out = self.df_city_pollutant.copy()

        df_out['date_local']=pd.to_datetime(df_out['date_local'],errors='coerce')

        df_out=(df_out.groupby(['date_local','site_number','sample_duration'])['sample_measurement']
        .mean()
        .reset_index()
        .set_index('date_local')
        .sort_index()
        )
        self.df_transformed=df_out
    
    def get_df_for_site(self,site_code):
        """filters grouped data for site (str)"""
        if self.df_transformed is None:
            raise ValueError("Run transform_data first")
        out_df=self.df_transformed.copy()
        out_df = out_df[
        out_df['site_number'] == site_code
        ]
        return out_df


    