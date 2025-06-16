"""
 * FILENAME: [aws_s3_functionality.py]
 * AUTHOR: [Jeremy Underwood]
 * COURSE: [CYOP 400]
 * PROFESSOR: [Craig Poma]
 * CREATEDATE: [21JAN25]
"""

import logging
import string
import random
import os
import re
import datetime
import sys
import boto3
from botocore.exceptions import ClientError

# Create an S3 client
s3 = boto3.client('s3')

# Catch Exceptions and write them to log file in current folder
# This log config is based on an example within the Week 2 Assignmen Guidance Announcement
logging.basicConfig(filename='./error.log',
                    format='%(asctime)s %(levelname)s:%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filemode='w',  # Overwrite the log file
                    level=logging.INFO)
logging.info(' ******* Object to upload! *******')

bucket_list = s3.list_buckets()
buckets = [bucket['Name']for bucket in bucket_list['Buckets']]


def validate_name(name):
    """Validate that the entered name is DNS-safe and
    that the name complies with S3 naming rules.
    """
    response = s3.list_buckets()
    bucket_names = [bucket['Name'] for bucket in response['Buckets']]
    matching_buckets = [n for n in bucket_names if name in n]
    if matching_buckets:
        print("Bucket name already exists!")
        return False
    pattern = re.compile('^[a-z-]+$')
    return bool(pattern.match(name))


def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False

    This code is based on the create_bucket.py within the S3Code zip file
    """

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    print(f"Bucket {bucket_name} has been created!")
    return True


# Function to generate a unique bucket name
def generate_bucket_name(firstname, lastname):
    """Generates a name for a bucket using the provided names
    and joining them with six random digits
    """

    suffix = ''.join(random.choices(string.digits, k=6))
    return f"{firstname}{lastname}-{suffix}"


def list_buckets():
    """Lists all s3 buckets
    """
    # Call S3 to list current buckets
    response = s3.list_buckets()
    # Get a list of all bucket names from the response
    buckets = [bucket['Name']for bucket in response['Buckets']]
    # Print out the bucket list
    print ("Bucket List: %s" % buckets)


def range_of_buckets(bucket_name):
    """Verify if the given bucket name is within s3 list of current buckets
    """

    bucket_list = s3.list_buckets()
    buckets = [bucket['Name']for bucket in bucket_list['Buckets']]

    if bucket_name not in buckets:
        return False
    return True


def upload_file(bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then same as file_name
    :return: True if file was uploaded, else False

    This code is based on the upload_file.py within the S3Code zip file
    """

    # Ensure file_name is a string and check if the file exists
    env_path = os.getcwd()
    file_name = 'error.log'
    file_path = os.path.join(env_path, file_name)

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    print(f"The {object_name} object has been uploaded to the {bucket} bucket!")
    return True


def list_bucket_objects(bucket_name):
    """List the objects in an Amazon S3 bucket

    :param bucket_name: string
    :return: List of bucket objects. If error, return None.

    This code is based on the list_objects.py within the S3Code zip file
    """

    # Retrieve the list of bucket objects
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            objects = []
            for obj in response['Contents']:
                objects.append(obj['Key'])
        else:
            print(f"There are no objects in the {bucket_name}!")
    except ClientError as e:
        # AllAccessDisabled error == bucket not found
        logging.error(e)
        return None
    return objects


def delete_object(bucket_name, object_name):
    """Delete an object from an S3 bucket

    :param bucket_name: string
    :param object_name: string
    :return: True if the referenced object was deleted, otherwise False

    This code is based on the delete_object.py within the S3Code zip file
    """

    try:
        s3.delete_object(Bucket=bucket_name, Key=object_name)
    except ClientError as e:
        logging.error(e)
        return False
    print(f"The {object_name} object was deleted from the {bucket_name} bucket!")
    return True


def delete_bucket(bucket_name):
    """Delete an empty S3 bucket

    If the bucket is not empty, the operation fails.

    :param bucket_name: string
    :return: True if the referenced bucket was deleted, otherwise False

    This code is based on the delete_bucket.py within the S3Code zip file
    """

    response = s3.list_objects_v2(Bucket=bucket_name)

    try:
        if 'Contents' in response:
            return None
        s3.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    print(f"The {bucket_name} bucket has been deleted!")
    return True


def copy_object(src_bucket_name, src_object_name,
                dest_bucket_name, dest_object_name=None):
    """Copy an Amazon S3 bucket object

    :param src_bucket_name: string
    :param src_object_name: string
    :param dest_bucket_name: string. Must already exist.
    :param dest_object_name: string. If dest bucket/object exists, it is
    overwritten. Default: src_object_name
    :return: True if object was copied, otherwise False

    This code is based on the copy_object.py within the S3Code zip file
    """

    # Construct source bucket/object parameter
    copy_source = {'Bucket': src_bucket_name, 'Key': src_object_name}
    if dest_object_name is None:
        dest_object_name = src_object_name

    # Copy the object
    s3 = boto3.client('s3')
    try:
        s3.copy_object(CopySource=copy_source, Bucket=dest_bucket_name,
                       Key=dest_object_name)
    except ClientError as e:
        logging.error(e)
        return False
    print(f"{src_object_name} was copied from {src_bucket_name} to {dest_bucket_name}!")
    return True


def download_object(bucket_name, object_name):
    """Download an object to the Cloud9 environment from a bucket
    """
    try:
        file_name = 'obj_download'
        s3.download_file(bucket_name, object_name, file_name)
    except ClientError as e:
        logging.error(e)
        return False
    print(f"The {object_name} object has been downloaded to the local environment as {file_name}!")
    return True


# Display the menu and handle user input
# Run the menu function in a loop to keep the program running
while True:
    print("\nAWS S3 Menu")
    print("1. Create Bucket")
    print("2. Upload Object")
    print("3. Delete Object")
    print("4. Delete Bucket")
    print("5. Copy Object")
    print("6. Download Object")
    print("7. Exit")

    choice = input("Enter choice: ")

    if choice == '1':
        firstname = input("Enter your first name: ")
        lastname = input("Enter your last name: ")
        if not firstname and not lastname:
            print("Please enter a name!")
            continue
        validate = validate_name(firstname + lastname)
        if validate is not True:
            print("Name not valid!")
            continue
        bucket_name = generate_bucket_name(firstname, lastname)
        create_bucket(bucket_name)
    elif choice == '2':
        list_buckets()
        bucket_name = input("Select a bucket and enter the name: ")
        upload_file(bucket_name)
    elif choice == '3':
        list_buckets()
        bucket_name = input("Select a bucket and enter the name: ")
        if range_of_buckets(bucket_name) == False:
            print("Bucket not in range!")
            continue
        objects = list_bucket_objects(bucket_name)
        print(f"Object List: {objects}")
        object_name = input("Enter object name to delete: ")
        if object_name not in objects:
            print('Object name out of range!')
            continue
        delete_object(bucket_name, object_name)
    elif choice == '4':
        list_buckets()
        bucket_name = input("Enter name of desired bucket to delete: ")
        if range_of_buckets(bucket_name) == False:
            print("Bucket not in range!")
            continue
        if delete_bucket(bucket_name) == None:
            print("Bucket should be empty before deleting!")
            continue
        else:
            delete_bucket(bucket_name)
    elif choice == '5':
        list_buckets()
        src_bucket_name = input("Enter source bucket name: ")
        if range_of_buckets(src_bucket_name) == False:
            print("Source bucket not in range!")
            continue
        print(list_bucket_objects(src_bucket_name))
        src_object_name = input("Enter object name to copy: ")
        objects = list_bucket_objects(src_bucket_name)
        if src_object_name not in objects:
            print('Object name out of range!')
            continue
        list_buckets()
        dest_bucket_name = input("Enter destination bucket name: ")
        if src_bucket_name == dest_bucket_name:
            print("Cannot copy to the same bucket!")
            continue
        if range_of_buckets(dest_bucket_name) == False:
            print("Destination bucket not in range!")
            continue
        copy_object(src_bucket_name, src_object_name, dest_bucket_name)
    elif choice == '6':
        list_buckets()
        src_bucket_name = input("Select a bucket and enter the name: ")
        print(list_bucket_objects(src_bucket_name))
        src_object_name = input("Enter object name to copy: ")
        download_object(src_bucket_name, src_object_name)
    elif choice == '7':
        print("Exiting program.")
        print(f"The date and time is {datetime.datetime.now()}")
        sys.exit()
    else: print("Invalid choice. Please choose a number one through seven.")
