import json
import boto3
import requests

def lambda_handler(event, context):
    S3_BUCKET_NAME = "rearc-thirdparty-datahub-us-east-2"
    DATA_SOURCE = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
    FOLDER_NAME = "population/"

    # Initialize S3 resource and get the bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3_BUCKET_NAME)

    # Request the API data and parse it
    r = requests.get(DATA_SOURCE)
    data = r.text

    # Upload the data to S3 with folder structure
    bucket.put_object(Key=FOLDER_NAME + "population.json", Body=data)


    return {
        'statusCode': 200,
        'body': json.dumps('Data has been extracted to JSON File successfully!')
    }
