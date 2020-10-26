#!/usr/bin/python3

import sys
import os
from datetime import datetime

arguments_count = len(sys.argv) - 1

# check two required arguments passed:
if arguments_count != 2:

    raise TypeError(f"upload_files_to_aws_s3 takes 2 positional arguments "\
                    f"({arguments_count} given)"
                    )

# proceed with two given arguments passed:
else:

    import boto3
    from botocore.exceptions import ClientError
    
    # create low-level client interface to AWS:
    s3_client = boto3.client("s3",
                             aws_access_key_id=\
                             os.environ["AWS_ACCESS_KEY_ID"],
                             aws_secret_access_key=\
                             os.environ["AWS_SECRET_KEY"]
                             )
    
    # command line arguments passed:
    base_dir_path = sys.argv[1]
    bucket = sys.argv[2]

    # directory (path) variables:
    base_dir = os.path.basename(base_dir_path)
    base_dir_start = base_dir_path.rstrip(base_dir)

    # counters:
    file_upload_count = 0
    file_delete_count = 0

    # define S3 bucket 'file-checker' function (file exists – True/False):
    def file_in_s3_bucket(s3_client, bucket, key):
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
        except ClientError:
            return False
        else:
            return True

    # define custom Exception class (if local and S3 files conflict):
    class ValidationError(Exception):
        def __init__(self, msg):
            self.msg = msg

    # iterate through base directory children (i.e. 'locations'):
    for dirname in os.listdir(base_dir_path):

        # check base directory child is a directory:
        if os.path.isdir(f"{base_dir_path}/{dirname}"):

            # iterate through base directory child files and (sub)folders:
            for root, dirs, files in os.walk(f"{base_dir_path}/{dirname}"):

                # iterate through files in (sub)folders 
                # of each base directory child:
                for filename in files:

                    # obtain local file path:
                    filepath_local = os.path.join(root, filename)

                    # set s3 upload file path:
                    filepath_s3_upload = os.path.relpath(filepath_local,
                                                         base_dir_start
                                                         )
                    
                    # check file doesn't already exist in S3 bucket:
                    if not file_in_s3_bucket(s3_client, 
                                             bucket, 
                                             filepath_s3_upload
                                             ):

                        # upload file to S3 bucket:
                        s3_client.upload_file(filepath_local, 
                                              bucket, 
                                              filepath_s3_upload
                                              )
                        
                        timestamp_uploaded = datetime.now()
                        timestamp_log = datetime.\
                                        strftime(timestamp_uploaded,
			                           "%b %d %H:%M:%S"
			                           )
                        
                        # output (print to terminal / write to log):
                        print(f"{timestamp_log} process: upload "\
                              f"bucket: {bucket} path: {filepath_s3_upload}"
                              )
                        
                        file_upload_count += 1
                        
                    # check file uploaded to S3 bucket successfully 
                    # (for delete local file):
                    if file_in_s3_bucket(s3_client, 
                                         bucket, 
                                         filepath_s3_upload
                                         ):

                        # get file info (e.g. byte size) of local file:
                        file_stat = os.stat(filepath_local)
                        # get file info (e.g. byte size) of S3 uploaded file:
                        file_head = s3_client.\
                                    head_object(Bucket=bucket,
                                                Key=filepath_s3_upload
                                                )
                        # validate local file is same as S3 uploaded file:
                        if file_head["ContentLength"] == file_stat.st_size:

                            # delete local file:
                            try:
                                os.remove(filepath_local)
                                
                                timestamp_deleted = datetime.now()
                                timestamp_log = datetime.\
                                                strftime(timestamp_deleted,
			                                   "%b %d %H:%M:%S"
			                                   )
                                
                                # output (print to terminal / write to log):
                                print(f"{timestamp_log} "\
                                      f"process: delete "\
                                      f"path: {filepath_local}"
                                      )
                                
                                file_delete_count += 1

                            except Exception as e:
                                print(e)

                        # should uploaded S3 file be diff. to local file:
                        else:
                           print(f"ValidationError: {filename} in S3 "\
                                 f"bucket has different file size to "\
                                 f"{filename} stored locally"
                                 )

                    else:
                        raise FileNotFoundError(f"S3 object "\
                                                f"'{filepath_s3_upload}' "\
                                                f"(bucket '{bucket}') "\
                                                 "unavailable at call to "\
                                                 "head_object()"
                                                )
    
    # output (print to terminal / write to log):
    if file_upload_count == 0 and file_delete_count == 0:

        timestamp_no_process = datetime.now()
        timestamp_log = datetime.\
                        strftime(timestamp_no_process,
                                 "%b %d %H:%M:%S"
                                 )
        print(f"{timestamp_log} "\
              f"process: null"
              )
