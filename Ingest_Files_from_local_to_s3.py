import boto3  # pip install boto3
import glob
import os
# Let's use Amazon S3
s3 = boto3.resource("s3")
# Print out bucket names
for bucket in s3.buckets.all():
    print("Exting Buckets : ", bucket.name)


BUCKET_NAME = 'ultimatealpr'
#FOLDER_NAME_IMAGE = 'recognizer_images'
FOLDER_NAME_LOG = 'recognizer_logs'

print(f"The {BUCKET_NAME} S3 bucket will be use to store log files ")

# Create session :
session = boto3.Session(profile_name='default')
s3 = session.client('s3')

# Pash for local files :
#image_files  = glob.glob("/media/iad/Nouveau nom/Data_Engineering_Nanodegree_Program/Capstone_project/recognizer_images/*.png")
log_files = glob.glob("/media/iad/Données/recognizer/recognizer_logs/*.json")

# Upload files from local to s3
for filename in log_files:
    key = "%s/%s" % (FOLDER_NAME_LOG, os.path.basename(filename))
    print("Putting %s as %s" % (filename,key))
    s3.upload_file(filename, BUCKET_NAME, key)

"""
for filename in log_files:
    key = "%s/%s" % (FOLDER_NAME_IMAGE, os.path.basename(filename))
    print("Putting %s as %s" % (filename,key))
    s3.upload_file(filename, BUCKET_NAME, key)
"""


print("All_Done")
