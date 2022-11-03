import json
import pandas as pd
from schema import get_schema, schema_to_json,market_to_json
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
    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,X-Amz-Security-Token,Authorization,X-Api-Key,X-Requested-With,Accept,Access-Control-Allow-Methods,Access-Control-Allow-Origin,Access-Control-Allow-Headers",
    "Access-Control-Allow-Origin": '*',
    "Access-Control-Allow-Methods": "DELETE,GET,HEAD,OPTIONS,PATCH,POST,PUT",
    "X-Requested-With": "*"
}

db = connect_to_db()
s3 = boto3.resource('s3')
def lambda_handler(event, context):

    insert_data_into_db()
    return {
        "status": "OK",
        "message": "INSERTED SUCCESSFULLY",
    }

#////////////////////////////////////////////////////////
"""main function for read csv file and return the json """
#////////////////////////////////////////////////////////

def insert_data_into_db():
    
    
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
        market="en_GB"
        obj = s3.Object(s3_bucket_name,file)
        data=obj.get()['Body'].read()
        if file == "users/Pinko/PINKO_en_GB (4).csv":
            df.append(pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False,encoding = "ISO-8859-1"))
            for file in df:
                pinko_data1 = pd.DataFrame(data = file)
                pinko_data = pd.DataFrame(np.concatenate([pinko_data.values, pinko_data1.values]), columns=pinko_data.columns)
                print(pinko_data, 'pinko-data')
            upcs = []
            demo=pinko_data['Product_ID']
            pid2=pinko_data['Product_ID'].iloc[0]
            i=0
            for pid in demo:
                y=pid
                if y != pid2:
                    pid2=y
                    i=0    
                if i==0:    
                    for index, row in pinko_data.iterrows():
                        if row.Product_ID == pid:
                                upcs.append(row)
                    file_data = json.dumps(schema_to_json(upcs))
                    db.set("Pinko" + "_" + market +"_"+ pid, file_data)
                    print("inserted",market)
                    upcs.clear()
                    i=i+1

            for index, row in pinko_data.iterrows():
                pid = row.Product_ID
                if row.Product_ID == pid:
                    if pid != pid2:
                        upcs.append(row)
                        file_d = json.dumps(market_to_json(upcs))
                        db.set("Pinko"+ "_" + market, file_d)
                        pid2=pid


