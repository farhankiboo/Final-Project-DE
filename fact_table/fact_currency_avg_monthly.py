import pandas as pd
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine
import logging

engine = create_engine("postgresql://postgres:''@localhost:1234/postgres")

if __name__ == '__main__':
    currencies = pd.read_sql(f"select * from dwh_final_1.topic_currency", con=engine)
    dim_currencies = pd.read_sql(f"select * from dwh_final_1.dim_currency", con=engine)

    cols = ['currency_id', 'rate', 'timestamp']
    fact_currency_monthly = currencies.loc[:, [cols[0], cols[1], cols[2]]]
    
    # Convert the 'timestamp' column to a datetime object
    fact_currency_monthly[cols[2]] = pd.to_datetime(fact_currency_monthly[cols[2]])

    # Create a new column 'month' with the formatted month
    # Month format: "%Y-%m"
    fact_currency_monthly['month'] = fact_currency_monthly['timestamp'].dt.strftime("%Y-%m")
    fact_currency_monthly = fact_currency_monthly.merge(dim_currencies, on="currency_id")
    fact_currency_monthly = fact_currency_monthly.drop(columns=['timestamp', 'currency_name'], axis=1)
    fact_currency_monthly = fact_currency_monthly.rename(columns={'rate':'monthly_rate_avg'}) 
    
    # Calculate the monthly average of the rates
    fact_currency_monthly_avg = fact_currency_monthly.groupby(['currency_id','month'])['monthly_rate_avg'].mean().reset_index()

    # Sort the dataframe by time
    fact_currency_monthly_avg = fact_currency_monthly_avg.sort_values('month')

    # Print the resulting dataframe
    print(fact_currency_monthly_avg)
  
    # Load to dwh
    try:
        res = fact_currency_monthly.to_sql('fact_currency_monthly', con=engine, schema='dwh_final_1', index=False, if_exists='replace')
        print(f'success insert data to table: fact_currency_monthly, inserted {res} data')
    except Exception as e:
        print('Failed to insert data to table: fact_currency_monthly')
        logging.error(e)