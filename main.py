import pprint

import boto3
from datetime import datetime
import json
import uuid


import pytz as pytz
# Initialize a boto3 S3 client
s3_client = boto3.client('s3')

# Cutoff date
CUTOFF_DATE = datetime(2023, 10, 15, tzinfo=pytz.UTC)


# Function to get all objects in an S3 bucket recursively
def get_all_objects(bucket_name):
    paginator = s3_client.get_paginator('list_objects_v2')
    rejected_count = 0
    accepted_count = 0
    for page in paginator.paginate(Bucket=bucket_name):
        for content in page.get('Contents', []):
            print(accepted_count, rejected_count, f"percentage: {(accepted_count+rejected_count)/1000000}")
            if content['LastModified'] > CUTOFF_DATE:
                accepted_count += 1
                yield content
            else:
                rejected_count += 1



# Function to read a JSON file from S3 and add the uploaded timestamp
def read_json_and_add_timestamp(bucket_name, object_key, last_modified):
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    file_content = response['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    json_content['timestamp'] = last_modified.isoformat()
    return json_content


# Function to group JSONs by hour of upload
def group_jsons_by_hour(bucket_name):
    jsons_by_hour = {}

    # Iterate over all objects in the bucket that are newer than the cutoff date
    for obj in get_all_objects(bucket_name):
        key = obj['Key']
        last_modified = obj['LastModified']

        # Only process if the object is a JSON file
        if key.endswith('.json'):
            json_dict = read_json_and_add_timestamp(bucket_name, key, last_modified)

            # Format the hour for grouping
            hour = last_modified.strftime('%Y-%m-%dT%H:00:00')

            # Add to the correct hour array in our grouped object
            if hour not in jsons_by_hour:
                jsons_by_hour[hour] = []
            jsons_by_hour[hour].append(json_dict)

    return jsons_by_hour

# replace with your bucket name
# grouped_jsons = group_jsons_by_hour("lomi-shadow-update-production")
# pprint.pprint(grouped_jsons)

import boto3
import os
# Initialize a session using Amazon S3
s3 = boto3.client('s3')

# Replace 'your-bucket-name' with your actual bucket name
bucket_name = 'lomi-shadow-update-production'

# Create an empty array to store the objects' metadata
s3_objects = {}
ignored = 0

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
        else:
            ignored += 1

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
