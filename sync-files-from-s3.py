import json
import os

import boto3


def load_config(config_path=os.path.join(os.path.dirname(__file__), 'download_config.json')):
    with open(config_path, 'r', encoding='UTF-8') as file:
        config = json.load(file)
    return config


def download_files_from_s3(client, local_path, s3_bucket, s3_directory):
    if not os.path.exists(local_path):
        print('Making download directory "%s" ...' % local_path)
        os.makedirs(local_path)
    paginator = client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_directory):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key[-1] == '/':
                    continue
                absolute_path = os.path.join(local_path, *(key.split('/')[2:-1]))
                if not os.path.exists(absolute_path):
                    os.makedirs(absolute_path)
                client.download_file(
                    Bucket=s3_bucket, Key=key, Filename=os.path.join(absolute_path, key.split('/')[-1])
                )


def main():
    config = load_config()
    client = boto3.client(
        's3',
        aws_access_key_id=config['aws_access_key_id'],
        aws_secret_access_key=config['aws_secret_access_key'],
        region_name=config['aws_region_name']
    )
    for path in config['path']:
        download_files_from_s3(
            client,
            local_path=path['target'],
            s3_bucket=config['aws_s3_bucket_name'],
            s3_directory=os.path.join(config['aws_s3_prefix'], path['source']).replace('\\', '/')
        )


if __name__ == '__main__':
    main()
