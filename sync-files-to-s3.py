import json
import os

import boto3
import botocore


def load_config(config_path=os.path.join(os.path.dirname(__file__), 'upload_config.json')):
    with open(config_path, 'r', encoding='UTF-8') as file:
        config = json.load(file)
    return config


def empty_s3_directory(client, s3_directory, s3_bucket):
    paginator = client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_directory):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                print('Deleting %s ...' % key)
                client.delete_object(Bucket=s3_bucket, Key=key)


def upload_files_to_s3(client, local_directory, s3_bucket, s3_directory='', del_pre_upload=False):
    if del_pre_upload:
        empty_s3_directory(client, s3_directory=s3_directory, s3_bucket=s3_bucket)
    for root, dirs, files in os.walk(local_directory):
        for file in files:
            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, local_directory)
            s3_path = os.path.join(s3_directory, relative_path).replace('\\', '/')
            print('Searching "%s" in "%s"' % (s3_path, s3_bucket))
            try:
                client.head_object(Bucket=s3_bucket, Key=s3_path)
                print('File found, skipped %s' % s3_path)
            except botocore.exceptions.ClientError:
                print('Uploading %s ...' % s3_path)
                client.upload_file(file_path, s3_bucket, s3_path)


def main():
    config = load_config()
    client = boto3.client(
        's3',
        aws_access_key_id=config['aws_access_key_id'],
        aws_secret_access_key=config['aws_secret_access_key'],
        region_name=config['aws_region_name']
    )
    for path in config['path']:
        upload_files_to_s3(
            client,
            local_directory=path['source'],
            s3_bucket=config['aws_s3_bucket_name'],
            s3_directory=os.path.join(config['aws_s3_directory_name'], path['target']).replace('\\', '/'),
            del_pre_upload=True
        )


if __name__ == '__main__':
    main()
