import json
import pandas as pd
from schema import get_schema, schema_to_json
from db import connect_to_db                                                                                                        
import boto3
import csv
import os
import io
import codecs
import sys
import numpy as np

headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Origin": '*',
    "Access-Control-Allow-Methods": 'POST'
}


# #Preloader function
# _file = pd.read_excel(f'./feed.xlsx', sheet_name="Worksheet")
# df = pd.DataFrame(_file, columns=get_schema())
# for index, row in df.iterrows():
#  db.set(row.PRODUCT_ID, json.dumps(schema_to_json(row)))
# s3_cient = boto3.client('s3')
db = connect_to_db()
s3 = boto3.resource('s3')

def lambda_handler(event, context):
    print("mayur made changes here")
    if event.get('body') == None:
        response = {
            "statusCode": 404,
            "headers": headers,
            "body": json.dumps({"message": "no body"}),
        }
        return response

    body = json.loads(event.get('body'))
    print(body,"mayur")
    pid = body.get('pid')
    print(pid,"mayur is here ")

    if 'pid' == None:
        return {
            "statusCode": 404,
            "headers": headers,
            "body": json.dumps({"message": "no pid"})
        }

    db_obj = load_from_db(pid)
    if db_obj is None:
        file_data = json.dumps(read_csv_from_s3(pid))
        db.set(pid, file_data)
    else:
        return {
            'statusCode': 200,
            "headers": headers,
            "body": json.dumps(db_obj)
        }
    return {
        'statusCode': 200,
        "headers": headers,
        "body": file_data
    }


def load_from_db(pid):
  db_obj = db.get(pid)
  if db_obj is None: return None
  json_data = json.loads(db_obj.decode('utf-8'))
  return json_data

def read_csv_from_s3(pid):
    
    
    s3_client =boto3.client('s3')
    s3_bucket_name='pushpendra.demo.csvtojson'
    
    
    my_bucket=s3.Bucket(s3_bucket_name)
    bucket_list = []
    for file in my_bucket.objects.filter(Prefix = 'pushpendra-dev/'):
        file_name=file.key
        print(file_name,"this is file key")
        if file_name.find(".csv")!=-1:
            bucket_list.append(file.key)
    length_bucket_list=print(len(bucket_list))
    print(bucket_list)
    
    
    #/////////////////////////////////////////
    
    if sys.version_info[0] < 3:
        from io import StringIO  # Python 3.x
    
    df = []
    converted_df = pd.DataFrame(columns=get_schema())
    
    # Initializing empty list of dataframes
    
    for file in bucket_list:
        obj = s3.Object(s3_bucket_name,file)
        data=obj.get()['Body'].read()
        if file == "pushpendra-dev/PINKO_en_GB (4).csv":
            df.append(pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False,encoding = "ISO-8859-1"))
            for file in df:
                converted_df1 = pd.DataFrame(data = file)
                converted_df = pd.DataFrame(np.concatenate([converted_df.values, converted_df1.values]), columns=converted_df.columns)
            upcs = []
            print(converted_df)
            for index, row in converted_df.iterrows():
                # print(row.Product_ID, "row id test")
                # print(pid, "row pid test")
                if row.Product_ID == pid:
                    upcs.append(row)
            if len(upcs) > 0:
               return schema_to_json(upcs)

            
        
    return {
        "status": "BAD",
        "message": "Could not find specified PID",
    }
