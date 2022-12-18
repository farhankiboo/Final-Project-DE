import pandas as pd
from sqlalchemy import create_engine
import logging

if __name__ == '__main__':
    engine = create_engine("postgresql://postgres:''@localhost:1234/postgres")

    # Extract Data
    company = pd.read_sql(f"select * from dwh_final_1.companies", con=engine)

    # Transform Data
    company.columns
    cols = ['country_code']
    dim_country = company[cols].groupby(cols).count().reset_index().reset_index()
    dim_country = dim_country.rename(columns={"index":"country_id"})
    dim_country = dim_country[dim_country.country_code != '']

    # Load Data
    try:
        res = dim_country.to_sql('dim_country', con=engine, schema='dwh_final_1', index=False, if_exists='replace')
        print(f'Success insert {res} data to table: dim_country')
    except Exception as e:
        print('Failed to insert data to table: dim_country')
        logging.error(e)
    