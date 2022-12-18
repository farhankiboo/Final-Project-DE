import pandas as pd
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine
import logging

engine = create_engine("postgresql://postgres:''@localhost:1234/postgres")
#engine = create_engine("postgresql://postgres:admin@localhost:5445/postgres")

if __name__ == '__main__':
    currencies = pd.read_sql(f"select * from dwh_final_1.topic_currency", con=engine)
    dim_currencies = pd.read_sql(f"select * from dwh_final_1.dim_currency", con=engine)

    cols = ['currency_id', 'rate', 'timestamp']
    fact_currency_daily = currencies.loc[:, [cols[0], cols[1], cols[2]]]
    
    # Convert the 'timestamp' column to a datetime object
    fact_currency_daily[cols[2]] = pd.to_datetime(fact_currency_daily[cols[2]])

    # Create a new column 'date' with the formatted date
    # Date format: "%Y-%m-%d"
    fact_currency_daily['date'] = fact_currency_daily['timestamp'].dt.strftime("%Y-%m-%d")
    fact_currency_daily = fact_currency_daily.merge(dim_currencies, on='currency_id')
    fact_currency_daily = fact_currency_daily.drop(columns=['timestamp', 'currency_name'], axis=1)
    fact_currency_daily = fact_currency_daily.rename(columns={'rate':'daily_rate_avg'}) 
   
    # Calculate the daily average of the rates
    fact_currency_daily_avg = fact_currency_daily.groupby(['currency_id', 'date'])['daily_rate_avg'].mean().reset_index()

    # Sort the dataframe by time
    fact_currency_daily_avg = fact_currency_daily_avg.sort_values('date')

    # Print the resulting dataframe
    print(fact_currency_daily_avg)
    
    # Load to Data Warehouse (Schema: dwh) 
    try:
        res = fact_currency_daily_avg.to_sql('fact_currency_daily', con=engine, schema='dwh_final_1', index=False, if_exists='replace')
        print(f'success insert data to table: fact_currency_daily, inserted {res} data')
    except Exception as e:
        print('Failed to insert data to table: fact_currency_daily')
        logging.error(e)
