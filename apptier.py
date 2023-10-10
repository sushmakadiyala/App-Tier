import boto3
import time
import os
import requests

REGION_NAME = 'us-east-1'
REQUEST_SQS='https://sqs.us-east-1.amazonaws.com/071576733573/cse546-cw-request-queue'
RESPONSE_SQS='https://sqs.us-east-1.amazonaws.com/071576733573/cse546-cw-output-queue'
INPUT_S3_BUCKET='cse546-cw-inputbucket'
OUTPUT_S3_BUCKET = 'cse546-cw-outputbucket'
INPUT_FOLDER = '/home/ubuntu/input_images'
RESULTS_FOLDER = '/home/ubuntu/classifier_results'

def sqs_client():
    sqs = boto3.client('sqs', region_name=REGION_NAME)
    return sqs
def s3_client():
    s3 = boto3.client('s3', region_name=REGION_NAME)
    return s3

def ec2_client():
    ec2 = boto3.client('ec2', region_name=REGION_NAME)
    return ec2


# This function downnloads the image from the input bucket corresponding to the input image name
def download_image_from_bucket(image_name):
    s3 = s3_client()
    # Use os.makedirs to create the directory along with parent directories if they don't exist
    os.makedirs(INPUT_FOLDER, exist_ok=True)
    downloaded_image_path = f'{INPUT_FOLDER}/{image_name}'
    s3.download_file(INPUT_S3_BUCKET, image_name, downloaded_image_path)
    return downloaded_image_path

# This function classifies the given image
def classify_image(downloaded_image_path):
    stdout = os.popen(f'cd /home/ubuntu/app-tier; python3 /home/ubuntu/app-tier/image_classification.py "{downloaded_image_path}"')
    classified_result  = stdout.read().strip()
    return classified_result 

# This function sends the result to response queue
def send_result_to_response_queue(result_key, classified_result):
    sqs = sqs_client()
    result_pair = f'({result_key},{classified_result})'
    sqs.send_message(
        QueueUrl=RESPONSE_SQS,
        MessageBody=result_pair,
        DelaySeconds=0
    )
# This function uploads the result to the output S3 bucket
def upload_result_to_output_bucket(result_key, classified_result):
    s3 = boto3.client('s3', region_name=REGION_NAME)
    result_pair = f'({result_key}, {classified_result})'
    s3.put_object(Key=result_key, Bucket=OUTPUT_S3_BUCKET, Body=result_pair)

def request_queue_length():
    request_sqs = sqs_client()
    length = request_sqs.get_queue_attributes(
        QueueUrl = REQUEST_SQS, 
        AttributeNames = ['ApproximateNumberOfMessages'])
    return int(length['Attributes']['ApproximateNumberOfMessages'])

def stop_current_instance():
    # Get the instance's metadata to determine its instance ID
    instance_id = requests.get('http://169.254.169.254/latest/meta-data/instance-id').text

    # Create an EC2 client
    ec2 = ec2_client()

    # Stop the EC2 instance
    response = ec2.stop_instances(InstanceIds=[instance_id])

def check_for_empty_queue():
    length1 = request_queue_length()
    if length1 == 0:
        time.sleep(10)
        length1 = request_queue_length()
        if length1 == 0:
            stop_current_instance()

while True:
    # receive a message from request queue
    check_for_empty_queue()
    # receive a message from request queue        
    sqs = sqs_client()
    received_message = sqs.receive_message(QueueUrl=REQUEST_SQS, MaxNumberOfMessages=1)
    images = received_message.get('Messages', [])

    for image in images:
        image_name = image['Body']
        receipt_handle = image['ReceiptHandle']  # Needed for message deletion

        #download corresponding image from input S3 bucket
        downloaded_image_path = download_image_from_bucket(image_name)

        #do classification using given model
        classified_result = classify_image(downloaded_image_path)

        #upload result to output s3 bucket
        result_key = image_name.split('.')[0]
        upload_result_to_output_bucket(result_key, classified_result)

        #send result to response queue 
        send_result_to_response_queue(result_key, classified_result)

        #delete image from local folder
        os.remove(downloaded_image_path)

        #delete message from request queue
        sqs.delete_message(QueueUrl=REQUEST_SQS, ReceiptHandle=receipt_handle)

    # Wait for 5 seconds before polling next message 
    time.sleep(8) 
