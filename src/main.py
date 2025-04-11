import json

from pathlib import Path
from pprint import pprint
from typing import Generator, Any, Union

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


def get_data(filename: Union[str, Path]) -> Generator[Any, Any, None]:
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


if __name__ == "__main__":
    with elastic_client(**settings.ES_CONFIG.model_dump(by_alias=True)) as es_client:
        # Получение списка индексов
        indices_names = [
            index["index"]
            for index in es_client.cat.indices(format="json")
            if not index["index"].startswith(".")
        ]
        # Удаление индексов
        if indices_names:
            es_client.indices.delete(index=indices_names, ignore_unavailable=True)

        for reader_data in get_data(settings.SAMPLES_PATH / "readers.jsonl"):
            ReaderElasticService(es_client).create(reader_data)

        for book_data in get_data(settings.SAMPLES_PATH / "books.jsonl"):
            BookElasticService(es_client).create(book_data)

        input()
        pprint(BookElasticService(es_client).get_expired_books())
        pprint(ReaderElasticService(es_client).get_total_books_read())
        #
        readers = ReaderElasticService(es_client).get_all(size=25)
        books = BookElasticService(es_client).get_all(size=25)

    input()
    with neo4j_client(
        **settings.NEO4J_CONFIG.model_dump(by_alias=True)
    ) as graph_client:
        CreateGraphScript(graph_client, readers, books).run()
        GetReadableWriterScript(graph_client).run()

    input()
    # Сохранение в HDFS
    with spark_client() as spark:
        readers_values = [
            (
                reader.id,
                reader.registration_date.strftime("%Y-%m-%d"),
                reader.fullname,
                reader.birthdate.strftime("%Y-%m-%d"),
                reader.address,
                reader.email,
                reader.education,
            )
            for reader in readers
        ]

        ReaderSparkService(spark, settings.SPARK_CONFIG.HADOOP_CONFIG).bulk_insert(
            readers_values
        )

        books_values = [
            (
                book.id,
                book.title,
                book.author,
                book.publishing_house,
                book.year_issue,
                book.language,
                book.shelf,
                issue.reader_id,
                issue.issue_date.strftime("%Y-%m-%d"),
                issue.return_date.strftime("%Y-%m-%d"),
                issue.return_factual_date.strftime("%Y-%m-%d"),
            )
            for book in books
            for issue in book.issue
        ]

        BookSparkService(spark, settings.SPARK_CONFIG.HADOOP_CONFIG).bulk_insert(
            books_values
        )
        GetDebtorsScript(spark, settings.SPARK_CONFIG.HADOOP_CONFIG).run().show()
        input()

    with pg_client(**settings.PG_CONFIG.model_dump(by_alias=True)) as pg_client:
        CreateTableScript(pg_client, readers).run()
        GetSimilarVectorsScript(pg_client).run()
