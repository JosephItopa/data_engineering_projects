# Importing libraries
import os
import json
#import boto3
import pandas as pd
import clickhouse_connect
import pyarrow.parquet as pq
from metaflow import FlowSpec, step

with open('config.json') as config_file:
    config = json.load(config_file)

class SingleTreeFlow(FlowSpec):

    @step
    def start(self):
        print("starting etl process...")
        self.next(self.extract_raw_data)

    @step
    def extract_raw_data(self):

        path = './tmp_raw_data'
        file_extension = '.parquet'
        csv_file_list = []
        dfs = []
        for root, dirs, files in os.walk(path):
            for name in files:
                if name.endswith(file_extension):
                    file_path = os.path.join(root, name)
                    csv_file_list.append(file_path)

        dfs = [pq.read_table(file).to_pandas() for file in csv_file_list]
        df = pd.concat(dfs, ignore_index=True)
        
        df.to_parquet("./tmp_processed_data/processing.parquet")
        self.next(self.transform_data)

    @step
    def transform_data(self):

        df = pd.read_parquet("./tmp_processed_data/processing.parquet")

        all_data = df.drop(["airport_fee", "Airport_fee", "lpep_pickup_datetime", "lpep_dropoff_datetime",\
                            "ehail_fee", "trip_type"], axis = 1) # 98% empty rows in the columns
        all_data = all_data.dropna()  # Remove rows with missing data
        all_data = all_data.rename(columns={'tpep_pickup_datetime': 'pickup_datetime', 'tpep_dropoff_datetime': 'dropoff_datetime'})
        all_data['pickup_datetime'] = pd.to_datetime(all_data['pickup_datetime'])
        all_data['dropoff_datetime'] = pd.to_datetime(all_data['dropoff_datetime'])
        all_data['trip_duration'] = all_data['dropoff_datetime'] - all_data['pickup_datetime']
        all_data['trip_duration_minutes'] = all_data['trip_duration'].dt.total_seconds() / 60
        
        all_df = all_data[['pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 
                        'store_and_fwd_flag', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
                        'tolls_amount', 'improvement_surcharge', 'total_amount', 'trip_duration']]
        
        all_df.columns = ['pickup_datetime', 'dropoff_datetime', 'passenger_count','trip_distance', 'rate_code_id', 
                    'store_and_fwd_flag','payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
                    'tolls_amount', 'improvement_surcharge', 'total_amount', 'trip_duration']

        part_df = all_df.head(100000)
        part_df.to_parquet("./tmp_cleaned_data/all_cleaned.parquet")
        self.next(self.load_data_to_clickhouse)

    @step
    def load_data_to_clickhouse(self):
        try :
            CLICKHOUSE_HOSTNAME = config["CLICKHOUSE_HOST"]
            CLICKHOUSE_USER = config["CLICKHOUSE_USER"]
            client = clickhouse_connect.get_client(
                                        host=CLICKHOUSE_HOSTNAME,
                                        port=9000,
                                        username=CLICKHOUSE_USER
                                        )
            print("connected to " + CLICKHOUSE_HOSTNAME + "\n")
        except:
            raise Exception("clickhouse connection error")
        
        all_df = pq.read_table("./tmp_cleaned_data/all_cleaned.parquet")
        
        # upload file
        try:
            client.command(
                'CREATE TABLE IF NOT EXISTS taxi_trips (pickup_datetime DateTime, dropoff_datetime DateTime, passenger_count UInt32,\
                trip_distance Float32, rate_code_id UInt32, store_and_fwd_flag String, payment_type UInt32, fare_amount Float32,\
                extra Float32, mta_tax Float32, tip_amount Float32, tolls_amount Float32, improvement_surcharge Float32,\
                total_amount Float32, trip_duration DateTime) ENGINE Memory')
            table_ = "taxi_trips"
            print(f"Table {table_} created or already exists")

            client.insert('taxi_trips', all_df, column_names=['pickup_datetime', 'dropoff_datetime', 'passenger_count',
                                                        'trip_distance', 'rate_code_id', 'store_and_fwd_flag',
                                                        'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
                                                        'tolls_amount', 'improvement_surcharge', 'total_amount',
                                                        'trip_duration'])

        except Exception as e:
            print("table not created due to"+e)
        self.next(self.end)

    @step
    def end(self):
        print("ending etl process...")


if __name__ == "__main__":
    SingleTreeFlow()

# https://www.digitalocean.com/community/tutorials/how-to-install-and-use-clickhouse-on-ubuntu-20-04
# https://stackoverflow.com/questions/60864973/new-to-clickhouse-cant-create-local-host