#!/usr/bin/env python
# coding: utf-8

from time import time

import argparse

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    csv_url = params.csv_url
    
    engine = create_engine(f'postgresql://{{user}}:root@localhost:5432/ny_taxi')

    df_iter = pd.read_csv('green_tripdata_2019-09.csv.gz', iterator=True, chunksize=100000)


    df = next(df_iter)


    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


    df.head(n=0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')

    df.to_sql(name='green_taxi_data', con=engine, if_exists='append')


    while True: 
        t_start = time()

        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name='green_taxi_data', con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))
    
parser = argparse.ArgumentParser(description='Ingesting CSV data into DB green_taxi')

#user, password, host, password
parser.add_argument('user', help='user name for postgres')
parser.add_argument('password', help='password for postgres')
parser.add_argument('host', help='host for postgres')
parser.add_argument('port', help='port for postgres')
parser.add_argument('db', help='db name for postgres')
parser.add_argument('table', help='table name for postgres')
parser.add_argument('csv_url', help='CSV url for ingest')

args = parser.parse_args()
print(args.accumulate(args.integers))

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

df_iter = pd.read_csv('green_tripdata_2019-09.csv.gz', iterator=True, chunksize=100000)


df = next(df_iter)


df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


df.head(n=0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')

df.to_sql(name='green_taxi_data', con=engine, if_exists='append')


while True: 
    t_start = time()

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    
    df.to_sql(name='green_taxi_data', con=engine, if_exists='append')

    t_end = time()

    print('inserted another chunk, took %.3f second' % (t_end - t_start))




