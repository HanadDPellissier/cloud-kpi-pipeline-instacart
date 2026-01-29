import os
import boto3
from config.settings import AWS_REGION, S3_BUCKET, s3_raw_prefix

def get_s3_client():
    # Uses default AWS credential chain:
    # env vars -> shared credentials file -> SSO -> instance role
    return boto3.client("s3", region_name=AWS_REGION)

def upload_csv_to_raw(table: str, local_path: str) -> tuple[str, int]:
    """
    Uploads a local CSV file to:
      s3://<bucket>/raw/<table>/load_date=<RUN_DATE>/<filename>

    Returns: (s3_key, bytes_uploaded)
    """
    s3 = get_s3_client()

    filename = os.path.basename(local_path)
    key = f"{s3_raw_prefix(table)}{filename}"

    s3.upload_file(local_path, S3_BUCKET, key)

    head = s3.head_object(Bucket=S3_BUCKET, Key=key)
    return key, int(head["ContentLength"])