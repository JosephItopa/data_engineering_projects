import os
import json
import boto3
import datetime
import random
import requests
from prefect import task, Flow
from pyspark.sql import SparkSession

with open('config.json') as config_file:
    config = json.load(config_file)

# Define the API key and base URL
WEATHER_API = config['WEATHER_API_KEY_SECRET']
ACCSESS_KEY_ID = config['AWS_KEY_ID']
SECRET_ACCESS_KEY = config['AWS_SECRET']
AWS_REGION = "us-east-1"

base_url = "https://api.openweathermap.org/data/2.5/weather"

cities_100 = ['New York', 'London', 'Paris', 'Tokyo', 'Sydney', 'Moscow', 'Beijing', 'Rio de Janeiro', 'Mumbai', 'Cairo', 'Rome', 'Berlin', 'Toronto', 'Lagos', 'Bangkok', 'Melbourne', 'Johannesburg']

# Initialize a Spark session
spark = SparkSession.builder.appName("WeatherData").getOrCreate()

# Define a function to fetch weather data for a city and return a Spark dataframe
@task
def fetch_weather_data(city):
    # Send a request to the OpenWeatherMap API for the city's weather data
    weather_data = None

    for city in cities:
        # Send a request to the OpenWeatherMap API for the city's weather data
        params = {"q": city, "appid": WEATHER_API, "units": "metric"}
        response = requests.get(base_url, params=params)
        data = response.json()

        # Extract the relevant weather data from the API response
        temp = data["main"]["temp"]
        humidity = data["main"]["humidity"]
        wind_speed = data["wind"]["speed"]

        # Create a Spark dataframe with the weather data for the city
        city_weather_data = spark.createDataFrame([(city, temp, humidity, wind_speed)],\
                                                  ["City", "Temperature", "Humidity", "WindSpeed"])
        if weather_data is None:
            weather_data = city_weather_data
        else:
            weather_data = weather_data.union(city_weather_data)
    return weather_data


# Use the fetch_weather_data function to fetch weather data for all cities and merge them into a single dataframe

def process_weather_data(weather_data):
    processed_data = weather_data.filter("Temperature > 10") \
                           .groupBy("City") \
                           .agg({"Humidity": "avg", "WindSpeed": "max"}) \
                           .withColumnRenamed("avg(Humidity)", "AverageHumidity") \
                           .withColumnRenamed("max(WindSpeed)", "MaxWindSpeed")
    return processed_data


s3_client = boto3.client('s3', region_name = 'us-east-1',\
                  aws_access_key_id = ACCSESS_KEY_ID, aws_secret_access_key = SECRET_ACCESS_KEY)

athena_client = boto3.client("athena", aws_access_key_id=ACCSESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY,\
                             region_name=AWS_REGION)

# create a bucket, database
bucket = s3_client.create_bucket(Bucket = 'weather-data')

athena_client.start_query_execution(QueryString='create database weatherdb',\
                                    ResultConfiguration={'OutputLocation': 's3://weather-data/queries/'})

DATABASE_NAME = "weather_db"
TABLE_DDL = "weather_table.ddl"
RESULT_OUTPUT_LOCATION = "s3://weather-data/queries/"

def create_bucket(bucket_name):
    # create a bucket
    try:
        bucket = s3_client.create_bucket(Bucket = bucket_name)
        print(f"{bucket_name} bucket successfully created.")
    except Exception as e:
        print(e)

def create_table(table_name, result_output_location):
    try:
        with open(TABLE_DDL) as ddl:
            response = athena_client.start_query_execution(QueryString = ddl.read(),\
                                                       ResultConfiguration = {"OutputLocation": result_output_location})
            print(f"{table_name} table successfully created.")
    except Exception as e:
        print(e)
    return response["QueryExecutionId"]

def create_database(database_name, table_name, result_output_location):
    try:
        response = athena_client.start_query_execution(QueryString=f"create database {database_name}",\
                                                   ResultConfiguration={"OutputLocation": result_output_location})
        print(f"{database_name} database successfully created.")
        create_table(table_name, result_output_location)
        print(f"{table_name} database successfully created.")
    except Exception as e:
        print(e)        
    return response["QueryExecutionId"]

# Write the weather data as a CSV file to a Google Cloud Storage bucket
@task
def write_weather_data_to_s3(bucket_name, weather_data):
    file_name = "current_weather_data.csv"
    try:
        create_bucket(bucket_name)
    except Exception as e:
        pass

    currentDateTime = datetime.now().strftime("%m-%d-%Y %H:%M:%S %p")
    weather_data.toPandas().to_csv(f"weather_data_{currentDateTime}.csv", index=False)

    path = './tmp'
    file_extension = '.csv'
    for root, dirs, files in os.walk(path):
        for name in files:
            if name.endswith(file_extension):
                file_path = os.path.join(root, name)
                # upload file
                try:
                    s3_client.upload_file(Filename = file_path, Bucket = bucket_name,\
                                          Key = 'weather_data')
                    print("uploaded successfully")
                except Exception as e:
                    print(e)

    return f"s3://{bucket_name}/{file_name}"

@task
def write_weather_data_to_athena(db_name, table, uri):
    
    create_database("weather_db", "weather_tbl", RESULT_OUTPUT_LOCATION)

   
with Flow("Weather Data Pipeline") as flow:

    cities = random.sample(cities_100, 10)
    weather_dat = fetch_weather_data(cities)
    processed_data = process_weather_data(weather_dat)
    uri = write_weather_data_to_s3("weather_app_storage", processed_data)
    write_weather_data_to_athena("weather_db", uri)

flow.run()
