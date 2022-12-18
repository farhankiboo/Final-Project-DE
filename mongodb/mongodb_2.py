import pymongo
import pandas as pd
import psycopg2
from bson.json_util import dumps, loads
from sqlalchemy import create_engine

if __name__ == "__main__" :
    # create engine
    user = 'dbDigitalSkola'
    password = '1234'
    CONNECTION_STRING = f"mongodb+srv://{user}:{password}@digitalskolaff.8vpbjbx.mongodb.net/test"
    engine = create_engine('postgresql://postgres:''@localhost:1234/postgres')

    # connect to mongodb
    client = pymongo.MongoClient(CONNECTION_STRING)
    db = client['sample_training']
    zips = db['zips']
    com = db['companies']

    # get data zips
    getZ = zips.find()
    dumpZ = dumps(list(getZ)).encode('utf-8')
    jsonZ = loads(dumpZ)

    # get data companies
    getC = com.aggregate([
    {'$unwind' : '$offices'},
    {'$project': {
                    'name' : 1,
                    'permalink' : 1,
                    'crunchbase_url' : 1,
                    'homepage_url' : 1,
                    'blog_url' : 1,
                    'blog_feed_url' : 1,
                    'twitter_username' : 1,
                    'category_code' : 1,
                    'number_of_employees' : 1,
                    'founded_year' : 1,
                    'founded_month' : 1,
                    'founded_day' : 1,
                    'deadpooled_year' : 1,
                    'deadpooled_month' : 1,
                    'deadpooled_day' : 1,
                    'deadpooled_url' : 1,
                    'tag_list' : 1,
                    'alias_list' : 1,
                    'email_address' : 1,
                    'phone_number' : 1,
                    'description' : 1,
                    'created_at' : 1,
                    'updated_at' : 1,
                    'overview' : 1,
                    'total_money_raised' : 1,
                    'office_description' : '$offices.description',
                    'office_address1' : '$offices.address1',
                    'office_address2' : '$offices.address2',
                    'office_zip_code' : '$offices.zip_code',
                    'office_city' : '$offices.city',
                    'office_state_code' : '$offices.state_code',
                    'office_country_code' : '$offices.country_code',
                    'office_latitude' : '$offices.latitude',
                    'office_longitude' : '$offices.longitude'
        }}
    ])
    dumpC = dumps(list(getC)).encode('utf-8')
    jsonC = loads(dumpC)

    # create dataframe from json
    zipsDF = pd.json_normalize(jsonZ)
    compDF = pd.json_normalize(jsonC)

    # rename loc in zipsDF
    zipsDF = zipsDF.rename(columns={'loc.y': 'latitute', 'loc.x': 'longitute'})

    # change _id dtype
    zipsDF['_id'] = zipsDF['_id'].astype(str)
    compDF['_id'] = compDF['_id'].astype(str)

    # insert to postgres
    zipsDF.to_sql('zips', engine, if_exists='replace', schema='dwh_final_1', index=False)
    compDF.to_sql('companies', engine, if_exists='replace', schema='dwh_final_1', index=False)
    print('dump complete')