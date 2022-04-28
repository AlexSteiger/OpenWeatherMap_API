# Python program to find current
# weather details of any city
# using openweathermap api
 
# import required modules
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
 
# Give city name
#Turkey Fields:
lat = "40.137442"
lon = "28.383499"
#Kassow, Germany:
#lat = "53.869024"
#lon = "12.079214"


# complete_url variable to store complete url address
complete_url = base_url + "appid=" + api_key + "&lat=" + lat + "&lon=" + lon + "&exclude=minutely,hourly,alerts"
print(complete_url)
    
# get method of requests module return response object
response = requests.get(complete_url)
 
# convert json format data into python format data:
x = response.json()

print(type(x))
#print(json.dumps(x, sort_keys=True, indent=4))

# Since x is a dictionary (<class 'dict'>) pd.read_json can be skipped
# https://stackoverflow.com/questions/44980845/importing-json-into-pandas

# read the response (x) into a dataframe:
df = pd.DataFrame(x['daily'])
#print(df)

# normalize nested 'temp' data:
df_temp = pd.json_normalize(df['temp'])
#print("df_temp: ")
#print(df_temp)

# subset of the dataframe:
df      = df[["dt","humidity","dew_point","wind_speed","clouds","rain"]]
df_temp = df_temp['day']

# concat the two dataframes horizontally:
df = pd.concat([df, df_temp], axis=1)

# rename time column:
df = df.rename(columns={"dt":"date"})

# convert from unix time to python datetime:
df['date'] = pd.to_datetime(df['date'],unit='s')
#print(df)

# change the datetime format to YYYY-MM-DD:
df['date'] = df['date'].dt.strftime("%Y-%m-%d")
#print(df)

postgreSQLTable = "turkey_weather";
alchemyEngine   = create_engine('postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/addferti_weather', pool_recycle=3600);
                # create_engine(dialect+driver://username:password@host:port/database)

postgreSQLConnection = alchemyEngine.connect();

try:
  print("try...")
  psql_df = pd.read_sql('select * from turkey_weather', con=postgreSQLConnection)
  #print("from psql: ")
  #print(psql_df)
  df_unique = pd.concat([df,psql_df]).drop_duplicates(subset=['date'],keep=False)
  print(len(df_unique), "new rows added to the database")
  frame = df_unique.to_sql(postgreSQLTable, postgreSQLConnection, index=False, if_exists='append');
except:
  print("except...")
  print("create table", postgreSQLTable)
  frame = df.to_sql(postgreSQLTable, postgreSQLConnection, index=False, if_exists='fail');
finally:
  postgreSQLConnection.close();
