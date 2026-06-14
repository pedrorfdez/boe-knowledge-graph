from dagster import asset, AssetExecutionContext
from pipeline.resources import MinioResource


@asset(description="Creates the legislation-lake MinIO bucket if it does not exist.")
def legislation_lake_bucket(context: AssetExecutionContext, minio: MinioResource) -> None:
    client = minio.get_client()
    if not client.bucket_exists(minio.bucket):
        client.make_bucket(minio.bucket)
        context.log.info(f"Created bucket: {minio.bucket}")
    else:
        context.log.info(f"Bucket already exists: {minio.bucket}")
