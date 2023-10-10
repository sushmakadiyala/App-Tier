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
AWS_ACCESS_KEY = 'AKIARBKSOI6C6Q2AMVY5'
AWS_SECRET_KEY = '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'

def sqs_client():
    sqs = boto3.client('sqs', region_name=REGION_NAME, aws_access_key_id=AWS_ACCESS_KEY,
                  aws_secret_access_key=AWS_SECRET_KEY)
    return sqs
def s3_client():
    s3 = boto3.client('s3', region_name=REGION_NAME, aws_access_key_id=AWS_ACCESS_KEY,
                  aws_secret_access_key=AWS_SECRET_KEY)
    return s3

def ec2_client():
    ec2 = boto3.client('ec2', region_name=REGION_NAME, aws_access_key_id=AWS_ACCESS_KEY,
                  aws_secret_access_key=AWS_SECRET_KEY)
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
    s3 = boto3.client('s3', region_name=REGION_NAME, aws_access_key_id=AWS_ACCESS_KEY,
                  aws_secret_access_key=AWS_SECRET_KEY)
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
    response = ec2.terminate_instances(InstanceIds=[instance_id])

    # Wait for the instance to reach the "stopped" state
    ec2.get_waiter('instance_stopped').wait(InstanceIds=[instance_id])

def check_for_empty_queue():
    length1 = request_queue_length()
    if length1 == 0:
        time.sleep(10)
        length1 = request_queue_length()
        if length1 == 0:
            stop_current_instance()

def get_running_instances():
    ec2 = ec2_client()

    # Get a list of all EC2 instances in the region
    response = ec2.describe_instances()

    # Initialize an empty list to store instance IDs in stopped state
    running_instance_ids = []

    # Iterate through the instances and check their state
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            instance_id = instance['InstanceId']
            state = instance['State']['Name']
            
            # Check if the instance is in running state
            if state == 'running':
                running_instance_ids.append(instance_id)

    
    # Print the list of instance IDs in stopped state
    print("EC2 instances in running state:")
    for instance_id in running_instance_ids:
        print(instance_id)

    return len(running_instance_ids)

while True:
    count = get_running_instances()
    if count > 2:
        check_for_empty_queue()
    # receive a message from request queue        
    sqs = sqs_client()
    received_message = sqs.receive_message(QueueUrl=REQUEST_SQS, MaxNumberOfMessages=1)
    images = received_message.get('Messages', [])

    for image in images:
        unique_name = image["Body"]
        client_ip, image_name = unique_name.split(' ')
        receipt_handle = image['ReceiptHandle']  # Needed for message deletion

        #download corresponding image from input S3 bucket
        downloaded_image_path = download_image_from_bucket(image_name)

        #do classification using given model
        classified_result = classify_image(downloaded_image_path)

        #upload result to output s3 bucket
        result_key = image_name.split('.')[0]
        upload_result_to_output_bucket(result_key, classified_result)

        unique_result_key = f"{client_ip} {result_key}"
        #send result to response queue 
        send_result_to_response_queue(unique_result_key, classified_result)

        #delete image from local folder
        os.remove(downloaded_image_path)

        #delete message from request queue
        sqs.delete_message(QueueUrl=REQUEST_SQS, ReceiptHandle=receipt_handle)

    # Wait for 5 seconds before polling next message 
    time.sleep(8)
