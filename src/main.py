import json

from pathlib import Path
from pprint import pprint
from typing import Generator, Any

from pyspark.sql.functions import col

from core.config import settings
from core.database import elastic_client, pg_client, spark_client, neo4j_client
from db.repositories.spark import ReaderSparkRepository, BookSparkRepository
from services.elastic.reader import ReaderElasticService
from services.elastic.book import BookElasticService
from services.rpc.neo4j.create_graph import CreateGraphScript
from services.rpc.neo4j.get_readable_writer import GetReadableWriterScript
from services.rpc.postgres.create_table import CreateTableScript
from services.rpc.postgres.get_similar_vectors import GetSimilarVectorsScript
from services.rpc.postgres.insert_values import InsertValuesScript

samples_path = Path(__file__).parent.parent / "samples"


def get_data(filename: str | Path) -> Generator[Any, Any, None]:
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


if __name__ == "__main__":
    with elastic_client(**settings.ES_CONFIG.model_dump(by_alias=True)) as es_client:
        for index in es_client.cat.indices(format="json"):
            index_name = index["index"]
            es_client.indices.delete(index=index_name, ignore_unavailable=True)

        for reader_data in get_data(samples_path / "readers.jsonl"):
            ReaderElasticService(es_client).create(reader_data)

        for book_data in get_data(samples_path / "books.jsonl"):
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
        # TODO: перевести на сервисы
        readers_values = [(reader.model_dump().values()) for reader in readers]
        books_values = []
        for book in books:
            for issue in book.issue:
                books_values.append(
                    (
                        book.id,
                        book.title,
                        book.author,
                        issue.reader_id,
                        issue.return_date,
                        issue.return_factual_date,
                    )
                )

        readers_repo = ReaderSparkRepository(spark)
        books_repo = BookSparkRepository(spark)

        readers_repo.create(readers_values)
        books_repo.create(books_values)

        readers_df = readers_repo.get_all()
        books_df = books_repo.get_all()

        # Преобразуем данные для поиска задолженности
        debtors_df = readers_df.join(
            books_df, on=readers_df["read_book_id"] == books_df["id"], how="inner"
        ).filter(
            (col("issue.return_factual_date").isNull())
            | (col("issue.return_factual_date") > col("issue.return_date"))
        )

        # Показать результаты
        debtors_df.show()

    with pg_client(**settings.PG_CONFIG.model_dump(by_alias=True)) as pg_client:
        CreateTableScript(pg_client).run()
        InsertValuesScript(pg_client, readers).run()
        GetSimilarVectorsScript(pg_client).run()
