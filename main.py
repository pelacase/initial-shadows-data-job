import uuid
from datetime import datetime

import boto3
import os

import pytz as pytz

s3 = boto3.client('s3')

bucket_name = 'lomi-shadow-update-production'
CUTOFF_DATE = datetime(2023, 10, 15, tzinfo=pytz.UTC)

s3_objects = {}
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=bucket_name, PaginationConfig={'MaxItems': 10000}):
    files = page.get('Contents', [])
    for obj in files:
        last_modified = obj['LastModified']
        if last_modified > CUTOFF_DATE:
            if last_modified.year not in s3_objects:
                s3_objects[last_modified.year] = {}
            if last_modified.month not in s3_objects[last_modified.year]:
                s3_objects[last_modified.year][last_modified.month] = {}
            if last_modified.day not in s3_objects[last_modified.year][last_modified.month]:
                s3_objects[last_modified.year][last_modified.month][last_modified.day] = {}
            if last_modified.hour not in s3_objects[last_modified.year][last_modified.month][last_modified.day]:
                s3_objects[last_modified.year][last_modified.month][last_modified.day][last_modified.hour] = []
            s3_objects[last_modified.year][last_modified.month][last_modified.day][last_modified.hour].append(obj)

for year in s3_objects:
    for month in s3_objects[year]:
        for day in s3_objects[year][month]:
            os.makedirs(f"output/year={year}/month={month}/day={day}")
            for hour in s3_objects[year][month][day]:
                hour_string = ""
                for obj in s3_objects[year][month][day][hour]:
                    s3_object = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
                    s3_object_as_string = s3_object['Body'].read().decode('utf-8')
                    with_recieved_time = s3_object_as_string[:-1] + f', "receivedtime": "{obj["LastModified"].isoformat()}"}}\n'
                    hour_string += with_recieved_time
                filename = f"output/year={year}/month={month}/day={day}/lomi-shadow-redshift-production-1-{year}-{month}-{day}-{hour}-00-{uuid.uuid4()}"
                with open(filename, "w") as f:
                    f.write(hour_string)
                    print(f"written {filename}")
