import pandas as pd
from sqlalchemy import create_engine
import logging

if __name__ == '__main__':
    engine = create_engine("postgresql://postgres:''@localhost:1234/postgres")

    # Extract Data
    company = pd.read_sql(f"select * from dwh_final_1.companies", con=engine)
    dim_country = pd.read_sql(f"select * from dwh_final_1.dim_country", con=engine)

    # Transform Data
    cols = ['state_code', 'country_code']
    dim_state = company[cols].groupby(cols).count().reset_index().reset_index()
    dim_state = dim_state.rename(columns={"index":"state_id"})
    dim_state = dim_state[dim_state.country_code != '']
    dim_state = dim_state.merge(dim_country, on='country_code').drop(columns='country_code')
    dim_state = dim_state[dim_state.state_code != '']
    dim_state.head(2)

    # Load Data
    try:
        res = dim_state.to_sql('dim_state', con=engine, index=False, if_exists='replace')
        print(f'Success insert {res} data to table: dim_state')
    except Exception as e:
        print('Failed to insert data to table: dim_state')
        logging.error(e)