import boto3
import requests
from bs4 import BeautifulSoup
import json

def lambda_handler(event, context):
    # Configuration
    S3_BUCKET_PATH = "rearc-thirdparty-datahub-us-east-2"
    BLS_FOLDER_PATH = "bls/"
    POPULATION_FOLDER_PATH = "population/"
    BLS_API = "https://download.bls.gov/pub/time.series/pr/"
    DATA_SOURCE = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"

    # Initialize S3 client
    s3 = boto3.client('s3')

    # Function to download and upload BLS files
    def process_bls_data():
        # Get list of objects in the S3 bucket
        bucket_objects = [obj['Key'] for obj in s3.list_objects(Bucket=S3_BUCKET_PATH, Prefix=BLS_FOLDER_PATH)['Contents']] 

        # List to track deleted objects
        deleted_list = bucket_objects.copy()

        # Request the data source and parse it
        headers = {"User-Agent": "MyDataDownloader (shreeshma09@gmail.com)"}
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
                s3.put_object(Bucket=S3_BUCKET_PATH, Key=BLS_FOLDER_PATH + file_name, Body=file_dl.content)
            # If the file exists in S3
            elif file_name in bucket_objects:
                # Get the S3 file
                s3_response = s3.get_object(Bucket=S3_BUCKET_PATH, Key=BLS_FOLDER_PATH + file_name)
                s3_file_content = s3_response['Body'].read()
                # If the S3 file is different from the website file, update the S3 file
                if file_dl.content != s3_file_content:
                    s3.put_object(Bucket=S3_BUCKET_PATH, Key=BLS_FOLDER_PATH + file_name, Body=file_dl.content)
                # Remove the file from the deleted list
                deleted_list.remove(BLS_FOLDER_PATH + file_name)

        # Remove files from S3 that are no longer on the website
        for file in deleted_list:
            if file != BLS_FOLDER_PATH + "population.json":  # To keep the "population.json" file
                s3.delete_object(Bucket=S3_BUCKET_PATH, Key=file)

        return 'BLS files have been uploaded to S3 successfully!'

    # Function to process population data
    def process_population_data():
        # Initialize S3 resource and get the bucket
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(S3_BUCKET_PATH)

        # Request the API data
        r = requests.get(DATA_SOURCE)
        data = r.text

        # Upload the data to S3 with folder structure
        bucket.put_object(Key=POPULATION_FOLDER_PATH + "population.json", Body=data)

        return 'Population data has been extracted to JSON File successfully!'

    # Process BLS data
    bls_result = process_bls_data()

    # Process population data
    population_result = process_population_data()

    return {
        'statusCode': 200,
        'body': json.dumps({'BLS_processing_status': bls_result, 'Population_processing_status': population_result})
    }
