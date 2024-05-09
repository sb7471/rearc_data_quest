import boto3
import requests
from bs4 import BeautifulSoup
import json

def lambda_handler(event, context):
    # Configuration
    S3_BUCKET_PATH = "rearc-thirdparty-datahub-us-east-2"
    FOLDER_PATH = "bls/"
    BLS_API = "https://download.bls.gov/pub/time.series/pr/"

    # Initialize S3 client
    s3 = boto3.client('s3')

    # Get list of objects in the S3 bucket
    bucket_objects = [obj['Key'] for obj in s3.list_objects(Bucket=S3_BUCKET_PATH, Prefix=FOLDER_PATH)['Contents']] 

    # List to track deleted objects
    deleted_list = bucket_objects.copy()

    # Request the data source and parse it
    headers = {"User-Agent": "MyDataDownloader (xxx@gmail.com)"}
    r = requests.get(BLS_API, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    for link in soup.find_all("a"):
        # Download the current file
        file_name = link.get_text()
        if file_name == "[To Parent Directory]":
            continue
        file_dl = requests.get(BLS_API + file_name, headers=headers)
        
        # If the file doesn't exist in S3, upload it
        if file_name not in bucket_objects:
            s3.put_object(Bucket=S3_BUCKET_PATH, Key=FOLDER_PATH + file_name, Body=file_dl.content)
        # If the file exists in S3
        elif file_name in bucket_objects:
            # Get the S3 file
            s3_response = s3.get_object(Bucket=S3_BUCKET_PATH, Key=FOLDER_PATH + file_name)
            s3_file_content = s3_response['Body'].read()
            # If the S3 file is different from the website file, update the S3 file
            if file_dl.content != s3_file_content:
                s3.put_object(Bucket=S3_BUCKET_PATH, Key=FOLDER_PATH + file_name, Body=file_dl.content)
            # Remove the file from the deleted list
            deleted_list.remove(FOLDER_PATH + file_name)

    # Remove files from S3 that are no longer on the website
    for file in deleted_list:
        if file != FOLDER_PATH + "population.json":  # To keep the "population.json" file
            s3.delete_object(Bucket=S3_BUCKET_PATH, Key=file)

    return {
        'statusCode': 200,
        'body': json.dumps('Files have been uploaded to S3 successfully!')
    }
