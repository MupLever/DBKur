from contextlib import contextmanager
from typing import Any, Generator

import psycopg2
from elasticsearch import Elasticsearch
from psycopg2._psycopg import connection
from py2neo import Graph
from pyspark.sql import SparkSession
import findspark


# dependency
@contextmanager
def elastic_client(
    *, host: str = "localhost", port: int = 9200
) -> Generator[Elasticsearch, Any, None]:
    with Elasticsearch(f"http://{host}:{port}/") as client:
        yield client


# dependency
@contextmanager
def neo4j_client(
    *,
    host: str = "localhost",
    port: int = 7687,
    auth: tuple[str, str] = ("neo4j", "test"),
) -> Generator[Graph, Any, None]:
    client = None
    try:
        client = Graph(f"bolt://{host}:{port}", auth=auth)
        yield client
    finally:
        if client:
            del client


# dependency
@contextmanager
def spark_client() -> Generator[SparkSession, Any, None]:
    findspark.init()
    client = None
    try:
        client = SparkSession.builder.appName("csv").getOrCreate()
        yield client
    finally:
        if client:
            client.stop()


# dependency
@contextmanager
def pg_client(
    *,
    host: str = "localhost",
    port: int = 5432,
    dbname: str = "postgres",
    user: str = "postgres",
    password: str = "postgres",
) -> Generator[connection, Any, None]:
    with psycopg2.connect(f"{host=} {port=} {dbname=} {user=} {password=}") as client:
        yield client
