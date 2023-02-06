from email import message
import os
import json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from fastapi import FastAPI, File, UploadFile, Request
import datetime
import time
import warnings
from producer import amqp_connection
warnings.simplefilter(action='ignore', category=FutureWarning)

#TODO: Exception Handling

def connection_string_builder():
    '''In order to create the connection string from config file'''
    with open('config.json','r') as conf_file:
        configs = json.load(conf_file)
    CONNECTION_STRING = "postgresql://"+configs['username']+":"+configs['password']+"@"+configs['timescaledb_host']+":"+configs['timescaledb_port']+'/'+configs['database_name']
    return CONNECTION_STRING


def psql_querier(query,display=False):  
    '''Generic query helper function'''
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


def init_database():
    '''Intial Database creation'''
    db_str = connection_string_builder()
    engine = create_engine(db_str)
    if not database_exists(engine.url):
        create_database(engine.url)
    

def dataframe_to_timescaledb(df,table_name):
    '''To send the dataframe to timescaledb'''
    db_str = connection_string_builder()
    engine = create_engine(db_str)
    df.to_sql(table_name, engine, if_exists='append',index=False)

def tables_initializer():
    '''Creating First tables'''
    customer = """CREATE TABLE customers (name TEXT NOT NULL,description TEXT NOT NULL);"""
    status = psql_querier(customer)
    asset = """CREATE TABLE assets (name TEXT NOT NULL,customer TEXT NOT NULL);"""
    status = psql_querier(asset)

def csv_parser(file_path,asset_name):
    '''There is a difference between the csv format and expected datapoints format so this function handles both parsing and iteration.
    The initial compute time was 3-4 mins which was because I was concating the dataframes for each iteration. I ended up doing the operations
    on dictionary and then convert it to dataframe'''
    df = pd.read_csv(file_path)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')] #Removing Unnamed columns
    timeseries = list(df.columns)
    timeseries.remove('timestamp')
    timeseries_dict = {
        "name": timeseries,
        "asset" : [asset_name]*len(timeseries)
    }
    df_timeseries = pd.DataFrame(timeseries_dict)
    row_dict = {'timestamp': [],'asset':[], 'timeseries': [], 'value':[]}
    for column_name in timeseries:
        for _, row in df.iterrows():
            row_dict['timestamp'].append(row['timestamp'])
            row_dict['asset'].append(asset_name)
            row_dict['timeseries'].append(column_name)
            row_dict['value'].append(row[column_name])
    df_datapoints = pd.DataFrame(row_dict)
    result = df_datapoints.to_json(orient='records')
    message = result
    print('Sending Data to AMQP')
    #TODO: TO SEND IT TO RMQ
    # message = json.dumps(row_dict)
    amqp_connection(message,'rabbitmq')
    return df_timeseries, df_datapoints
time.sleep(10)
init_database()
tables_initializer()

app = FastAPI()

@app.post("/ingest_from_file")
async def ingestion_through_file(req: Request, file: UploadFile = File(...)):
    try:
        contents = file.file.read()
        customer_name = req.headers.get('customer_name')
        asset_name = req.headers.get('asset_name')
        with open(file.filename, 'wb') as f:
            f.write(contents)
    except Exception:
        return {"message": "There was an error uploading the file"}
    finally:
        file.file.close()
    timeseries, datapoints = csv_parser(file.filename,asset_name)
    dataframe_to_timescaledb(timeseries,'timeseries')
    # dataframe_to_timescaledb(datapoints,'datapoints')
    cust_list = psql_querier("""SELECT * FROM customers;""",True)
    flag = 0
    for cust in cust_list:
        if customer_name in cust[0]:
            flag = 1
    if flag == 0:
        psql_querier("""INSERT INTO customers (name, description) VALUES ('{0}','' );""".format(customer_name))
    existing_assets = psql_querier("""SELECT * FROM assets WHERE customer = '{0}';""".format(customer_name),True)
    if existing_assets == []:
        psql_querier("""INSERT INTO assets (name, customer) VALUES ('{0}','{1}' );""".format(asset_name,customer_name))
    os.remove(file.filename)
    psql_querier('ALTER TABLE datapoints ALTER COLUMN timestamp TYPE TIMESTAMP USING timestamp::timestamp;')
    return {"message": f"File Processed and Database updated"}


@app.post("/retrive_datapoints")
async def retrive_datapoints(req: Request):
    #Req 
    # body = {"asset":"C01","timeseries":['generator___mechanical___speed','inverter___temperature'],"daterange":['2012-06-30','2012-07-03']} 
    req_json_body = await req.body()
    req_json_body = req_json_body.decode('utf-8')
    req_dict = json.loads(req_json_body)
    query = """SELECT * from datapoints WHERE (asset = '{0}') AND (timeseries in {1}) AND (timestamp::date BETWEEN '{2}' AND '{3}');""".format(req_dict['asset'],str(tuple(req_dict['timeseries'])),req_dict['daterange'][0],req_dict['daterange'][1])
    query_out = psql_querier(query,True)
    df = pd.DataFrame(query_out, columns=(['timestamp','asset','timeseries','value']))
    result = df.to_json(orient="records")
    return result

@app.post("/verify_date_ts")
async def verify_date_ts(req: Request):
# request body = {"timeseries":"generator___mechanical___speed","daterange":["2012-06-30","2012-07-03"]}
    req_json_body = await req.body()
    req_json_body = req_json_body.decode('utf-8')
    req_dict = json.loads(req_json_body)
    query = """with dates as
            (
            SELECT
                timestamp, timeseries, value, timestamp::date as date
            FROM
                datapoints
            WHERE
                timeseries = '{0}'
            AND
                timestamp::date BETWEEN '{1}' AND '{2}'
            ) 
            SELECT DISTINCT
                SUBSTRING(CAST(date as varchar),1,10)
            FROM
                dates;""".format(req_dict['timeseries'],req_dict['daterange'][0],req_dict['daterange'][1])
    list_of_dates = psql_querier(query,True)
    date_list = []
    for j in list_of_dates:
        date_list.append(j[0])
    sdate = datetime.datetime.strptime(req_dict['daterange'][0],"%Y-%m-%d")
    edate = datetime.datetime.strptime(req_dict['daterange'][1],"%Y-%m-%d")
    dr = pd.date_range(sdate,edate-datetime.timedelta(days=1),freq='d')
    output_dictionary = {}
    for i in dr.values:
        for j in date_list:
            if pd.Timestamp(j) in dr.values:
                output_dictionary[j]='TRUE'
            else:
                output_dictionary[i.strftime("%Y-%m-%d")]='FALSE'
    return output_dictionary

@app.get("/list_customers")
async def list_customers():
    cust_list = psql_querier("""SELECT * FROM customers;""",True)
    cust_dict = {'Customer_Names':[]}
    for item in cust_list:
        cust_dict['Customer_Names'].append(item[0])
    return cust_dict


@app.post("/edit_customer_information")
async def edit_customer_information(req: Request):
    # Request body = {"customer_name":"reliance","updated_description":"test"}
    req_json_body = await req.body()
    req_json_body = req_json_body.decode('utf-8')
    req_dict = json.loads(req_json_body)
    query = """UPDATE customers
    SET description = '{0}'
    WHERE name = '{1}';""".format(req_dict['updated_description'],req_dict['customer_name'])
    psql_querier(query)
    return {"Success": "Description Updated"}

@app.post("/compute_stats")
async def compute_stats(req: Request):
    #Request body = {"customer_name":"reliance","timeseries":"generator___mechanical___speed","daterange":["2012-06-30","2012-07-03"]}
    req_json_body = await req.body()
    req_json_body = req_json_body.decode('utf-8')
    req_dict = json.loads(req_json_body)
    asset_query = """SELECT assets.name FROM assets, customers WHERE assets.customer = customers.name;"""
    asset_list = psql_querier(asset_query,True) 
    asset_tuple = ()
    for item in asset_list:
        asset_tuple = asset_tuple + (item[0],)
    if len(asset_tuple) == 1:
        query = """SELECT MAX(value),MIN(value),AVG(value),stddev(value),count(timestamp) FROM datapoints WHERE timeseries = '{0}' AND timestamp::date BETWEEN '{1}' AND '{2}' AND asset = '{3}';""".format(req_dict['timeseries'],req_dict['daterange'][0],req_dict['daterange'][1],asset_list[0][0])
    else:
        query = """SELECT MAX(value),MIN(value),AVG(value),stddev(value),count(timestamp) FROM datapoints WHERE timeseries = '{0}' AND timestamp::date BETWEEN '{1}' AND '{2}' AND asset IN {3};""".format(req_dict['timeseries'],req_dict['daterange'][0],req_dict['daterange'][1],str(asset_tuple))
    stats = psql_querier(query,True)
    stats_dict = {
        "max": stats[0][0],
        "min": stats[0][1],
        "avg": stats[0][2],
        "stddev": stats[0][3],
        "row_count": stats[0][4]
    }
    #NOTE: DROPPING FREQUENCY as we only had 3 hours of data
    
    return stats_dict

@app.post("/delete_datapoints")
async def delete_datapoints(req: Request):
    # Request body = {"timeseries":"generator___mechanical___speed", "daterange":["2012-07-01 09:00:01","2012-07-01 10:06:45"]}
    # DELETE FROM datapoints WHERE (timeseries = 'generator___mechanical___speed') AND (timestamp BETWEEN '2012-07-01 09:00:01'::timestamp AND '2012-07-01 10:06:45'::timestamp);
    req_json_body = await req.body()
    req_json_body = req_json_body.decode('utf-8')
    req_dict = json.loads(req_json_body)
    query = """DELETE FROM datapoints WHERE (timeseries = '{0}') AND (timestamp BETWEEN '{1}'::timestamp AND '{2}'::timestamp);""".format(req_dict['timeseries'],req_dict['daterange'][0],req_dict['daterange'][1])
    psql_querier(query)
    return {"Success":"Deleted datapoints between given timerange"}
    



