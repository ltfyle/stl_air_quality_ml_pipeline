import pandas as pd
import os

# get the data
df_NO2=pd.read_csv("stl_NO2_historic.csv")
df_PM25=pd.read_csv("stl_pm25_historic.csv")
df_weather=pd.read_csv("stl_weather_historic.csv")

# column renaming, minor formatting
df_NO2 = df_NO2.rename(columns={'date_local': 'date'})
df_PM25 = df_PM25.rename(columns={'date_local': 'date'})

# merging air quality data together
df_aq = pd.concat([df_NO2,df_PM25])
df_aq = df_aq.rename(columns={'date_local':'date'})

# PM25 is only measured once every three days
# since we have full data for 1 hour data, for both pollutants, 
# and the scope of this project is to make a daily forcast I will drop it
df_aq_clean=df_aq.loc[df_aq['samp_dur_hrs']==1]
df_aq_clean.drop(columns='samp_dur_hrs',inplace=True)

# transform data to have columns with mean daily numbers for each pollutant
df_aq_final = df_aq_clean.pivot_table(
    index="date",
    columns="pollutant",
    values="sample_measurement",
    aggfunc="mean"
).reset_index()

df_aq_final.columns.name = None

df_weather=df_weather.set_index('date')
df_aq_final=df_aq_final.set_index('date')

df_full = df_weather.join(df_aq_final, how="outer")

if os.path.exists("historic_data.csv"):
    print("file already exists")
else:
    df_full.to_csv("historic_data.csv")

