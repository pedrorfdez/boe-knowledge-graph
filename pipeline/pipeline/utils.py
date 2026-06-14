from pipeline.resources import MinioResource


def delta_storage_options(minio: MinioResource) -> dict[str, str]:
    return {
        "AWS_ENDPOINT_URL": minio.endpoint,
        "AWS_ACCESS_KEY_ID": minio.access_key,
        "AWS_SECRET_ACCESS_KEY": minio.secret_key,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
