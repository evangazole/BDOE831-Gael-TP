import boto3
from botocore.client import Config

s3 = boto3.resource('s3',
                    endpoint_url='http://localhost:9010',
                    aws_access_key_id='minioadmin',
                    aws_secret_access_key='minioadmin',
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')

print("Buckets:")
for bucket in s3.buckets.all():
    print(bucket.name)
    print("Contents:")
    for obj in bucket.objects.all():
         print(f" - {obj.key}")
