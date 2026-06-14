from contextlib import contextmanager
from dagster import ConfigurableResource
from neo4j import GraphDatabase
from minio import Minio
from qdrant_client import QdrantClient
from elasticsearch import Elasticsearch
import redis


class Neo4jResource(ConfigurableResource):
    uri: str
    username: str = "neo4j"
    password: str

    @contextmanager
    def get_driver(self):
        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        try:
            yield driver
        finally:
            driver.close()


class MinioResource(ConfigurableResource):
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str

    def get_client(self) -> Minio:
        # Minio client takes host:port only — strip scheme
        host = self.endpoint.replace("http://", "").replace("https://", "")
        secure = self.endpoint.startswith("https://")
        return Minio(host, access_key=self.access_key, secret_key=self.secret_key, secure=secure)


class QdrantResource(ConfigurableResource):
    url: str

    def get_client(self) -> QdrantClient:
        return QdrantClient(url=self.url)


class ElasticsearchResource(ConfigurableResource):
    url: str

    def get_client(self) -> Elasticsearch:
        return Elasticsearch(self.url)


class RedisResource(ConfigurableResource):
    url: str

    def get_client(self) -> redis.Redis:
        return redis.from_url(self.url)
