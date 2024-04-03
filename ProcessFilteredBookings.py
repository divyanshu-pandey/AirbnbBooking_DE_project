import boto3
import pandas as pd
from io import StringIO
import json
from botocore.exceptions import ClientError

# Initialize S3 client
s3_client = boto3.client('s3')

# Your S3 bucket and file name
bucket_name = 'airbnb-filtered-data-bucket-0'
file_name = 'airbnb-filtered-data.csv'

def lambda_handler(event, context):
    # Your JSON response
    json_response =event[0]['body']
    #'{"booking_id": "8Zv4Ab", "user_id": "2xywXA", "property_id": "JB0S7n", "location": "Mumbai, India","start_booking_date": "2023-09-19","end_booking_date": "2023-09-27", "price": 507.12, "booking_for_days": 8}'
    # Assuming the JSON response is passed in the event body
    json_data = json.loads(json_response)

    # Convert JSON to DataFrame
    new_data_df = pd.DataFrame([json_data])

    # Check if the file exists
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_name)
        # File exists, read existing data
        csv_obj = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        current_data = csv_obj['Body'].read().decode('utf-8')
        current_df = pd.read_csv(StringIO(current_data))
        # Append new data
        appended_data = pd.concat([current_df, new_data_df], ignore_index=True)
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            # Object does not exist, handle accordingly
            print(f"The object {file_name} does not exist in bucket {bucket_name}.")
        # File does not exist, use new data as the DataFrame
        appended_data = new_data_df

    # Convert DataFrame to CSV and upload to S3
    appended_data_encoded = appended_data.to_csv(None, index=False).encode('utf-8')
    s3_client.put_object(Body=appended_data_encoded, Bucket=bucket_name, Key=file_name)

    return {
        'statusCode': 200,
        'body': json.dumps('Data appended to CSV file in S3')
    }
