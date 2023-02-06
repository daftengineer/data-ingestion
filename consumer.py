import pika
import sys
from sqlalchemy import create_engine
import json
import pandas as pd
import time

def connection_string_builder():
    with open('config.json','r') as conf_file:
        configs = json.load(conf_file)
    CONNECTION_STRING = "postgresql://"+configs['username']+":"+configs['password']+"@"+configs['timescaledb_host']+":"+configs['timescaledb_port']+'/'+configs['database_name']
    return CONNECTION_STRING


def psql_querier(query,display=False):  
    db_str = connection_string_builder()
    engine = create_engine(db_str)
    out = []
    try:
        with engine.connect() as con:
            query_out = con.execute(query)
            if display == True:
                for item in query_out:
                    out.append(item)
                return out
    except Exception:
        return False
    else:
        return True

def dataframe_to_timescaledb(df,table_name):
    db_str = connection_string_builder()
    engine = create_engine(db_str)
    df.to_sql(table_name, engine, if_exists='append',index=False)


time.sleep(15)
credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq',credentials=credentials)) #TODO: to change with config.json
channel = connection.channel()

channel.exchange_declare(exchange='bulk_file_ingestion', exchange_type='topic')

result = channel.queue_declare('', exclusive=True)
queue_name = 'sensor_data'

binding_keys = sys.argv[1:]
if not binding_keys:
    sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
    sys.exit(1)

for binding_key in binding_keys:
    channel.queue_bind(
        exchange='bulk_file_ingestion', queue=queue_name, routing_key=binding_key)

print(' [*] Waiting for logs. To exit press CTRL+C')


def callback(ch, method, properties, body):
    df = pd.read_json(json.loads(body))
    print('sending data to sql')
    dataframe_to_timescaledb(df,'datapoints')
    print('sql ingest done: ',len(df))
    print(" [x] %r:\n%r" % (method.routing_key, df.head()))



channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()