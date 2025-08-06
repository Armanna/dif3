import boto3

"""
    gets credentials from environment variables
"""
class S3:

    def __init__(self):
        self.s3 = boto3.client('s3')

    def upload_file(self, file_path, bucket_name):
        self.s3.upload_file(file_path, bucket_name, file_path)

    def upload_fileobj(self, data, bucket_name, file_name):
        print("uploading file " + file_name + " to bucket " + bucket_name)
        self.s3.put_object(Body=data, Bucket=bucket_name, Key=file_name)
