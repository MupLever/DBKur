import json

from pathlib import Path
from pprint import pprint
from typing import Generator, Any

from core.config import settings
from core.database import elastic_client, pg_client, spark_client, neo4j_client
from services.elastic.reader import ReaderElasticService
from services.elastic.book import BookElasticService
from services.scripts.neo4j.create_graph import CreateGraphScript
from services.scripts.neo4j.get_readable_writer import GetReadableWriterScript
from services.scripts.spark.get_debtors import GetDebtorsScript
from services.spark.book import BookSparkService
from services.spark.reader import ReaderSparkService

from services.scripts.postgres.create_table import CreateTableScript
from services.scripts.postgres.get_similar_vectors import GetSimilarVectorsScript
from services.scripts.postgres.insert_values import InsertValuesScript


def get_data(filename: str | Path) -> Generator[Any, Any, None]:
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


if __name__ == "__main__":
    with elastic_client(**settings.ES_CONFIG.model_dump(by_alias=True)) as es_client:
        for index in es_client.cat.indices(format="json"):
            index_name = index["index"]
            es_client.indices.delete(index=index_name, ignore_unavailable=True)

        for reader_data in get_data(settings.SAMPLES_PATH / "readers.jsonl"):
            ReaderElasticService(es_client).create(reader_data)

        for book_data in get_data(settings.SAMPLES_PATH / "books.jsonl"):
            BookElasticService(es_client).create(book_data)

        pprint(BookElasticService(es_client).get_expired_books())
        pprint(ReaderElasticService(es_client).get_total_books_read())

        readers = ReaderElasticService(es_client).get_all(size=25)
        books = BookElasticService(es_client).get_all(size=25)

    with neo4j_client(
        **settings.NEO4J_CONFIG.model_dump(by_alias=True)
    ) as graph_client:
        CreateGraphScript(graph_client, readers, books).run()
        GetReadableWriterScript(graph_client).run()

    # Сохранение в HDFS
    with spark_client(**settings.SPARK_CONFIG.model_dump(by_alias=True)) as spark:
        readers_values = [(reader.model_dump().values()) for reader in readers]
        BookSparkService(spark, settings.SPARK_CONFIG).bulk_insert(readers_values)

        books_values = [
            (
                book.id,
                book.title,
                book.author,
                issue.reader_id,
                issue.return_date,
                issue.return_factual_date,
            )
            for book in books
            for issue in book.issue
        ]
        ReaderSparkService(spark, settings.SPARK_CONFIG).bulk_insert(books_values)

        GetDebtorsScript(spark, settings.SPARK_CONFIG).run().show()

    with pg_client(**settings.PG_CONFIG.model_dump(by_alias=True)) as pg_client:
        CreateTableScript(pg_client).run()
        InsertValuesScript(pg_client, readers).run()
        GetSimilarVectorsScript(pg_client).run()
