#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string"
}



@click.command()
@click.option('--pg_user', default='root', help='PostgreSQL username')
@click.option('--pg_password', default='root', help='PostgreSQL password')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_port', default='5432', help='PostgreSQL port')
@click.option('--pg_database', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year for data')
@click.option('--month', default=1, type=int, help='Month for data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
@click.option('--table_name', default='zone_taxi_data', help='Table name')
def run(pg_user, pg_password, pg_host, pg_port, pg_database, year, month, chunksize, table_name):
    prefix = 'https://d37ci6vzurychx.cloudfront.net/misc/'
    url = prefix + 'taxi_zone_lookup.csv'
    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}') 

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        iterator=True,
        chunksize=chunksize,
    )
    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.to_sql(name=table_name, con=engine, if_exists='replace')
            first = False
        else:
            df_chunk.to_sql(name=table_name, con=engine, if_exists='append')

if __name__ == '__main__':
    run()