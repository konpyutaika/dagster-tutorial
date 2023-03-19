from dagster import Config


class S3Config(Config):
    s3_bucket: str
    s3_prefix: str
