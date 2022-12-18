import json
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
import psycopg2


# connection to postgres
url = 'postgresql+psycopg2://postgres:''@localhost:1234/postgres'
engine = create_engine(url)

# create table in postgres
with engine.connect() as conn:
    conn.execute(text("""
            CREATE TABLE IF NOT EXISTS topic_currency (
                currency_id varchar,
                currency_name varchar,
                rate float,
                timestamp varchar);
            """))


consumer = KafkaConsumer(
                'TopicCurrency'
                , bootstrap_servers=['localhost:9092']
                # , auto_offset_reset='earliest'
                , api_version=(0,10)
                , value_deserializer = lambda m: json.loads(m.decode("utf-8"))
            )


for message in consumer:

    json_data = message.value

    event = {
        "currency_id": json_data["currency_id"],
        "currency_name": json_data["currency_name"],
        "rate": json_data["rate"],
        "timestamp": json_data["timestamp"]
    }

    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO topic_currency 
                VALUES (:currency_id, :currency_name, :rate, :timestamp)"""),
            [event]
        )