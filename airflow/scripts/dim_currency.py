import pandas as pd
from sqlalchemy import create_engine
import logging

if __name__ == '__main__':
    engine = create_engine("postgresql://postgres:''@localhost:1234/postgres")

    # Extract Data
    currency = pd.read_sql(f"select * from dwh_final_1.topic_currency", con=engine)

    # Transform Data
    cols = ['currency_id', 'currency_name']
    dim_currency = currency[cols].groupby(cols).count().reset_index()
    dim_currency

    # Load Data
    try:
        res = dim_currency.to_sql('dim_currency', con=engine, schema='dwh_final_1', if_exists='replace')
        print(f'Success insert {res} data to table: dim_currency')
    except Exception as e:
        print('Failed to insert data to table: dim_currency')
        logging.error(e)