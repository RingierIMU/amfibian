import sys
import boto3
from botocore.vendored import requests
import awswrangler as wr
from urllib import request, parse
import json
import datetime

def py_to_slack(text):
    post = {"text": "{0}".format(text)}
    try:
        # print('within...')
        json_data = json.dumps(post)
        req = request.Request("https://hooks.slack.com/services/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",
                              data=json_data.encode('ascii'),
                              headers={'Content-Type': 'application/json'}
        )
        resp = request.urlopen(req)
        # print("donsies")
    except Exception as em:
        print('ag nee')
        print("EXCEPTION: " + str(em))
    return resp

def get_db_changes():
    database = 'playweekdemo'
    todays_date = "2022-07-08 00:00:00" # datetime.date.today().strftime('%Y-%m-%d 00:00:00') # no data for the 8th
    query = f"select table_name, column_name, column_datatype from dbchange_staging" # where created_at >= cast('{todays_date}' as timestamp)"
    df = wr.athena.read_sql_query(sql=query, database=database, ctas_approach=False)
    print(df)
    return df

def lambda_handler(event, context):
    changes = get_db_changes()
    py_to_slack( str( changes ) )
    return {
        'statusCode': 200,
        'body': json.dumps(changes)
    }

