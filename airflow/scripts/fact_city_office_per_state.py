import pandas as pd
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine
import logging

engine = create_engine("postgresql://postgres:''@localhost:1234/postgres")

if __name__ == '__main__':
    companies = pd.read_sql(f"select * from dwh_final_1.companies", con=engine)

    fact_agg_state = companies.groupby('office_state_code').agg({'office_city': 'count', 'office_description': 'count'}).reset_index()
    fact_agg_state = fact_agg_state[fact_agg_state['office_state_code'] != '']

    try:
        res = fact_agg_state.to_sql('fact_city_office_per_state', con=engine, schema='dwh_final_1', index=False, if_exists='replace')
        print(f'Success insert data to table: fact_agg_state, inserted {res} data')
    except Exception as e:
        print('Failed to insert data to table: fact_agg_state')
        print(f'ERROR: {e}')