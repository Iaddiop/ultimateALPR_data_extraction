"""
File author: Ibrahima DIOP
Company: Doubango AI
LinkedIn: https://www.linkedin.com/in/ibrahima-diop-82636462/
Email: ibrahimadiop.idp@gmail.com
License: For non-commercial use only.
Source code: https://github.com/Iaddiop/ultimateALPR_data_extraction
"""

import boto3
import glob
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
BUCKET_NAME = 'ultimatealpr'
FOLDER_NAME_LOG = 'recognizer_logs'


def list_buckets():
    """
    Lists all Amazon S3 buckets associated with the AWS account.

    This function uses the Boto3 resource interface to retrieve and log
    the names of all S3 buckets available under the configured AWS account.
    """
    try:
        s3 = boto3.resource("s3")
        for bucket in s3.buckets.all():
            logging.info(f"Existing Bucket: {bucket.name}")
    except boto3.exceptions.Boto3Error as e:
        logging.error(f"Error listing buckets: {e}")


def upload_files_to_s3(bucket_name, folder_name, local_path_pattern):
    """
    Uploads files from a local directory to a specified folder in an S3 bucket.

    Parameters:
    - bucket_name (str): The name of the S3 bucket where files will be uploaded.
    - folder_name (str): The folder within the S3 bucket where files will be stored.
    - local_path_pattern (str): The glob pattern for selecting files from the local filesystem.

    This function establishes a session using Boto3 and uploads each file that matches
    the local_path_pattern to the specified S3 bucket and folder.
    """
    try:
        session = boto3.Session(profile_name='default')
        s3_client = session.client('s3')
        files = glob.glob(local_path_pattern)

        for filename in files:
            key = f"{folder_name}/{os.path.basename(filename)}"
            logging.info(f"Uploading {filename} as {key}")
            try:
                s3_client.upload_file(filename, bucket_name, key)
                logging.info(f"Successfully uploaded {filename} as {key}")
            except boto3.exceptions.S3UploadFailedError as e:
                logging.error(f"Error uploading {filename}: {e}")
    except boto3.exceptions.Boto3Error as e:
        logging.error(f"Error in S3 session: {e}")


def main():
    logging.info(f"The {BUCKET_NAME} S3 bucket will be used to store log files.")
    list_buckets()
    upload_files_to_s3(BUCKET_NAME, FOLDER_NAME_LOG, "/recognizer_logs/*.json")
    logging.info("All done.")


if __name__ == "__main__":
    main()
