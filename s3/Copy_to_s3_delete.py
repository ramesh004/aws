import boto3
from boto3.session import Session
# import time

_BUCKET_NAME = 'awsbucketcoke'
_PREFIX = 'lawson/ORACARCH01_DBBNBCD/'

id_='AxxxxxxxxxxL'
secret = 'hjxxxxxxxxxxxxO'
session = Session(
                aws_access_key_id=id_,
                aws_secret_access_key=secret,
                region_name='us-east-2'
            )

client = session.client('s3', verify=False)
s3_resource = session.resource('s3')
response = client.list_objects(Bucket=_BUCKET_NAME, Prefix=_PREFIX)
for content in response.get('Contents', []):
    if content.get('Key').endswith("parquet"):
        copy_source = {
            'Bucket': _BUCKET_NAME,
            'Key': content.get('Key')
        }
        new_name = content.get('Key').replace(".parquet","_2022.parquet")
        import time
        s3_resource.meta.client.copy(copy_source, _BUCKET_NAME, new_name)
        s3_resource.Object(_BUCKET_NAME, content.get('Key')).delete()





#
#
# def ListFiles(client):
#     """List files in specific S3 URL"""
#     response = client.list_objects(Bucket=_BUCKET_NAME, Prefix=_PREFIX)
#     for content in response.get('Contents', []):
#         yield content.get('Key')
#
# file_list = ListFiles(client)
# for file in file_list:
#     print ('File found: %s' % file)
