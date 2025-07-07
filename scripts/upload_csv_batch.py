import boto3
from pathlib import Path
import argparse
import yaml

def load_config():
    return yaml.safe_load(open('config/paths.yaml'))

def upload_if_new(local_file):
    cfg = load_config()
    bucket = cfg['bucket']
    prefix = cfg['input_prefix']
    s3 = boto3.client('s3')
    key = f"{prefix}/{Path(local_file).name}"
    try:
        s3.head_object(Bucket=bucket, Key=key)
        print(f"Skip upload, {key} exists.")
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3.upload_file(local_file, bucket, key)
            print(f"Uploaded {key}")
        else:
            raise

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Upload CSV batch to S3")
    parser.add_argument('--file', required=True, help='Local CSV file to upload')
    args = parser.parse_args()
    upload_if_new(args.file)
