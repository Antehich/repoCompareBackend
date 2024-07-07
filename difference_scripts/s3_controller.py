import boto3

session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net'
)
bucket_name = 'repo-compare-results'


def upload_result_into_storage(name, postfix, path):
    object_name = name + '_' + postfix + '.json'
    try:
        s3.upload_file(path, bucket_name, object_name, ExtraArgs={'ContentType': 'application/json'})
        print(f'{path} uploaded successfully to {bucket_name}/{object_name}')
        return f'{bucket_name}/{object_name}', postfix

    except Exception as e:
        print(f'Error uploading {path} to {bucket_name}/{object_name}: {e}')
