import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import os

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)


# Weather for the year 2025, for all the zipcodes available for saint louis,
url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
params = {
	"latitude": [38.855, 39.0469, 38.8772, 38.6283, 38.9444, 38.7095, 38.8067, 38.7961, 38.7609, 38.934, 38.9272, 38.7917, 38.6437, 38.9788, 38.8122, 38.2675, 38.509, 38.2322, 38.4296, 38.3915, 38.3655, 38.5282, 38.3421, 38.2103, 38.2606, 38.2492, 38.2783, 38.4829, 38.2804, 38.3094, 38.5845, 38.7336, 38.3453, 38.4501, 38.5012, 38.6114, 38.6272, 38.4371, 38.5971, 38.3357, 38.42, 38.357, 38.6542, 38.2232, 38.5883, 38.5937, 38.3344, 38.5941, 38.5042, 38.4948, 38.5867, 38.4904, 38.5773, 38.2307, 38.7761, 38.7148, 38.7537, 38.7655, 38.5613, 38.4685, 38.5561, 38.5406, 38.6315, 38.6265, 38.6312, 38.6091, 38.6399, 38.6446, 38.6611, 38.6445, 38.5897, 38.6142, 38.5733, 38.6538, 38.6618, 38.6953, 38.677, 38.59, 38.6278, 38.5982, 38.5804, 38.6845, 38.7088, 38.579, 38.5559, 38.6574, 38.551, 38.5431, 38.5513, 38.4846, 38.4886, 38.6578, 38.6035, 38.6683, 38.6708, 38.7272, 38.7398, 38.7074, 38.7533, 38.7703, 38.6058, 38.738, 38.6682, 38.615, 38.6238, 38.7415, 38.6981, 38.7185, 38.6261, 38.4677, 38.629, 38.6372, 38.6278, 38.6278, 38.6265, 38.6278, 38.6187, 38.629, 38.6693, 38.6278, 38.6278, 38.629, 38.629, 38.6278, 38.629, 38.6272, 38.6278, 38.6268, 38.6278, 38.6358, 38.6278, 38.6289, 38.6262, 38.7855, 38.7823, 38.7596, 38.7376, 38.5747, 38.7459, 38.6785, 38.6042, 38.812, 38.8133, 38.867, 38.996, 38.8788, 38.1655, 38.8075, 38.8025, 38.8071, 38.8182],
	"longitude": [-90.8631, -90.7438, -90.9689, -91.0562, -90.9183, -90.8806, -90.7019, -90.8055, -90.7709, -90.783, -90.3447, -90.6191, -91.1876, -90.9804, -91.1308, -90.3807, -90.5115, -90.5652, -90.5525, -90.3803, -90.363, -90.8483, -90.4089, -90.8212, -90.8032, -90.483, -90.6489, -90.7463, -90.3953, -90.753, -90.7545, -90.3778, -90.9809, -91.0091, -90.6275, -90.7349, -90.5184, -90.3669, -90.5455, -90.4039, -90.781, -90.6452, -90.5584, -90.382, -90.4971, -90.5655, -90.6938, -90.5144, -90.633, -90.4271, -90.6403, -90.8193, -90.6116, -90.7981, -90.3676, -90.4497, -90.4225, -90.4614, -90.4895, -90.8894, -91.0162, -90.4804, -90.1932, -90.1889, -90.2042, -90.2102, -90.3053, -90.2, -90.207, -90.2598, -90.3041, -90.2567, -90.2411, -90.2846, -90.2585, -90.3765, -90.2538, -90.2507, -90.3168, -90.234, -90.3357, -90.2692, -90.2887, -90.4089, -90.2819, -90.3569, -90.2783, -90.3724, -90.4089, -90.3883, -90.3474, -90.3073, -90.4337, -90.36, -90.2886, -90.3533, -90.3035, -90.2577, -90.1934, -90.182, -90.2922, -90.324, -90.4417, -90.3189, -90.3522, -90.3647, -90.464, -90.2334, -90.1934, -90.3055, -90.2058, -90.2449, -90.1996, -90.1996, -90.1889, -90.1996, -90.1987, -90.2058, -90.3998, -90.1996, -90.1996, -90.2058, -90.2058, -90.1996, -90.2058, -90.3115, -90.1996, -90.3113, -90.1996, -90.2452, -90.1996, -90.2048, -90.1975, -90.4858, -90.486, -90.5207, -90.6227, -90.8794, -90.6529, -90.8085, -90.9967, -91.1437, -90.858, -90.2226, -90.7403, -91.0007, -90.7364, -90.3607, -90.3111, -90.2978, -90.2866],
	"start_date": "2025-01-01",
	"end_date": "2025-12-31",
	"daily": ["temperature_2m_max", "precipitation_sum", "precipitation_hours", "uv_index_max", "wind_speed_10m_max", "shortwave_radiation_sum"],
	"timezone": "America/Chicago",
	"wind_speed_unit": "mph",
	"temperature_unit": "fahrenheit",
}
responses = openmeteo.weather_api(url, params = params)


# list to store location data
location_data = []
# Process 151 locations
for response in responses:
	print(f"\nCoordinates: {response.Latitude()}°N {response.Longitude()}°E")
	print(f"Elevation: {response.Elevation()} m asl")
	print(f"Timezone: {response.Timezone()}{response.TimezoneAbbreviation()}")
	print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")
	
	# Process daily data. The order of variables needs to be the same as requested.
	daily = response.Daily()
	daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
	daily_precipitation_sum = daily.Variables(1).ValuesAsNumpy()
	daily_precipitation_hours = daily.Variables(2).ValuesAsNumpy()
	daily_uv_index_max = daily.Variables(3).ValuesAsNumpy()
	daily_wind_speed_10m_max = daily.Variables(4).ValuesAsNumpy()
	daily_shortwave_radiation_sum = daily.Variables(5).ValuesAsNumpy()
	
	daily_data = {"date": pd.date_range(
		start = pd.to_datetime(daily.Time() + response.UtcOffsetSeconds(), unit = "s", utc = True),
		end =  pd.to_datetime(daily.TimeEnd() + response.UtcOffsetSeconds(), unit = "s", utc = True),
		freq = pd.Timedelta(seconds = daily.Interval()),
		inclusive = "left"
	)}
	daily_data["temperature_2m_max"] = daily_temperature_2m_max
	daily_data["precipitation_sum"] = daily_precipitation_sum
	daily_data["precipitation_hours"] = daily_precipitation_hours
	daily_data["uv_index_max"] = daily_uv_index_max
	daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
	daily_data["shortwave_radiation_sum"] = daily_shortwave_radiation_sum
	
	daily_dataframe = pd.DataFrame(data = daily_data)
	daily_dataframe["latitude"] = response.Latitude()
	daily_dataframe["longitude"] = response.Longitude()
	location_data.append(daily_dataframe)

# gluing together the dataframes for each location
historical_weather_df = pd.concat(location_data, ignore_index=True)
historical_weather_df = historical_weather_df.sort_values(["latitude","longitude","date"]).reset_index(drop=True)

# fix date format from iso to pandas datetime
historical_weather_df["date"] = (
    pd.to_datetime(historical_weather_df["date"])
      .dt.tz_localize(None)   # remove timezone
      .dt.date                # remove time (keep only day)
)

# simple agregation for each variable, by date
df_historical_weather_transformed=historical_weather_df.groupby(['date'])[['temperature_2m_max','precipitation_sum','precipitation_hours','uv_index_max','wind_speed_10m_max','shortwave_radiation_sum']].mean().reset_index()

if os.path.exists("stl_weather_historic.csv"):
	print("file already exists")
else:
	df_historical_weather_transformed.to_csv("stl_weather_historic.csv", index=False)

