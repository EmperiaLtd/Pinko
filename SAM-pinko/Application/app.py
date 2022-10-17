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

db = connect_to_db()
s3 = boto3.resource('s3')

def lambda_handler(event, context):
    if event.get('body') == None:
        response = {
            "statusCode": 404,
            "headers": headers,
            "body": json.dumps({"message": "no body"}),
        }
        return response

    body = json.loads(event.get('body'))
    pid = body.get('pid')
    market=body.get('market')

    if 'pid' == None:
        return {
            "statusCode": 404,
            "headers": headers,
            "body": json.dumps({"message": "no pid"})
        }

    db_obj = load_from_db(pid)
    # if db_obj is None:
    file_data = json.dumps(read_csv_from_s3(pid))
    db.set(pid + "_" + market, file_data) #set the data into database
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

#////////////////////////////////////////////////////////
"""main function for read csv file and return the json """
#////////////////////////////////////////////////////////

def read_csv_from_s3(pid):
    
    
    s3_client =boto3.client('s3')
    s3_bucket_name='sftpgw-i-06e8a0b5d0a44b1fb'
    
    
    my_bucket=s3.Bucket(s3_bucket_name)
    bucket_list = []
    for file in my_bucket.objects.filter(Prefix = 'users/Pinko/'):
        file_name=file.key
        if file_name.find(".csv")!=-1:
            bucket_list.append(file.key)
    
    if sys.version_info[0] < 3:
        from io import StringIO  # Python 3.x
    
    df = []
    pinko_data = pd.DataFrame(columns=get_schema())
    
    
    for file in bucket_list:
        obj = s3.Object(s3_bucket_name,file)
        data=obj.get()['Body'].read()
        if file == "users/Pinko/PINKO_en_GB (4).csv":
            df.append(pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False,encoding = "ISO-8859-1"))
            for file in df:
                pinko_data1 = pd.DataFrame(data = file)
                pinko_data = pd.DataFrame(np.concatenate([pinko_data.values, pinko_data1.values]), columns=pinko_data.columns)
            upcs = []
            for index, row in pinko_data.iterrows():
                if row.Product_ID == pid:
                    upcs.append(row)
            if len(upcs) > 0:
               return schema_to_json(upcs)

  
    return {
        "status": "BAD",
        "message": "Could not find specified PID",
    }
