# Python program to find current weather details 
# of an location using openweathermap api

import requests, json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
 
# curl query:
# curl "https://api.openweathermap.org/data/2.5/onecall?lat=51.509865&lon=-0.118092&exclude=minutely,hourly,alerts&appid=65d4508050d5008b768b660a688651ad" | python -mjson.tool
# Turkey Fields: 40.137442, 28.383499
    
# Enter your API key here
api_key = "65d4508050d5008b768b660a688651ad"
 
# base_url variable to store url
base_url = "https://api.openweathermap.org/data/2.5/onecall?"

# Variables

postgreSQLTable = ["bakir_weather","kassow_weather"]
lat = ["40.137442","53.869024"]
lon = ["28.383499","12.079214"]

for i in range(0, 2):
  print(postgreSQLTable[i])

  # Locations:
  # Bakir,Turkey Fields:
  # lat = "40.137442"
  # lon = "28.383499"
  # Kassow,Germany Fields:
  # lat = "53.869024"
  # lon = "12.079214"

  # complete_url variable to store complete url address
  complete_url = base_url + "appid=" + api_key + "&lat=" + lat[i] + "&lon=" + lon[i] + "&exclude=minutely,hourly,alerts"

  # get method of requests module return response object
  response = requests.get(complete_url)

  # convert json format data into python format data:
  x = response.json()

  #print(type(x))
  #print(json.dumps(x, sort_keys=True, indent=4))

  # read the response (x) into a dataframe:
  df = pd.DataFrame(x['daily'])

  # normalize nested 'temp' data:
  df_temp = pd.json_normalize(df['temp'])

  # add rain column, if not exists
  if 'rain' not in df.columns:
    df["rain"] = 0

  # subset of the dataframe:
  df      = df[["dt","humidity","dew_point","wind_speed","clouds","rain"]]
  df_temp = df_temp['day']

  # concat the two dataframes horizontally:
  df = pd.concat([df, df_temp], axis=1)

  # rename time column:
  df = df.rename(columns={"dt":"date","day":"temp"})
 
  # kelvin to celsius
  df['temp'] = df['temp'] - 273.15
  df['dew_point'] = df['dew_point'] - 273.15

  # fill NaN with Null
  df = df.fillna(0)

  # convert from unix time to python datetime:
  df['date'] = pd.to_datetime(df['date'],unit='s')

  #print(df.dtypes)
  print(df)

  alchemyEngine   = create_engine('postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/addferti_weather', pool_recycle=3600);
        # create_engine(dialect+driver://username:password@host:port/database)

  postgreSQLConnection = alchemyEngine.connect();

  try:
    print("try...")
    frame = df.to_sql(postgreSQLTable[i], postgreSQLConnection, index=False, if_exists='append')
    postgreSQLConnection.execute(
        "DELETE FROM {} t WHERE EXISTS (SELECT FROM {} WHERE date = t.date AND ctid < t.ctid);"
        .format(postgreSQLTable[i],postgreSQLTable[i]))
  except:
    print("except...")
    print("create table", postgreSQLTable[i])
    frame = df.to_sql(postgreSQLTable[i], postgreSQLConnection, index=False, if_exists='fail');
  finally:
    postgreSQLConnection.close();
