from dagster import Definitions, EnvVar
from pipeline.resources import (
    Neo4jResource, MinioResource, QdrantResource, ElasticsearchResource, RedisResource,
)
from pipeline.assets.minio_init import legislation_lake_bucket
from pipeline.assets.bronze import bronze_boe_norms

resources = {
    "neo4j": Neo4jResource(uri=EnvVar("NEO4J_URI"), password=EnvVar("NEO4J_PASSWORD")),
    "minio": MinioResource(
        endpoint=EnvVar("MINIO_ENDPOINT"),
        access_key=EnvVar("MINIO_ROOT_USER"),
        secret_key=EnvVar("MINIO_ROOT_PASSWORD"),
        bucket=EnvVar("MINIO_BUCKET"),
    ),
    "qdrant": QdrantResource(url=EnvVar("QDRANT_URL")),
    "elasticsearch": ElasticsearchResource(url=EnvVar("ELASTICSEARCH_URL")),
    "redis": RedisResource(url=EnvVar("REDIS_URL")),
}

defs = Definitions(assets=[legislation_lake_bucket, bronze_boe_norms], resources=resources)
