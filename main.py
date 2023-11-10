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
for page in paginator.paginate(Bucket=bucket_name, PaginationConfig={'PageSize': 10000, 'MaxItems': 10000}):
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
                hour_bytes = b''
                for obj in s3_objects[year][month][day][hour]:
                    s3_object = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
                    hour_bytes += s3_object['Body'].read()
                    with open(f"output/year={year}/month={month}/day={day}/lomi-shadow-redshift-production-1-{year}-{month}-{day}-{hour}-00-{uuid.uuid4()}", "wb") as f:
                        f.write(hour_bytes)
