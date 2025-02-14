import json
from collections import deque
from pprint import pprint

from pathlib import Path
from pyspark.sql.functions import col

from core.config import settings
from core.pipeline import Pipeline
from core.database import elastic_client, pg_client, neo4j_client, spark_client
from elastic.jobs import JobCleanIndicies, JobCreateIndex
from elastic.queries import expired_books, total_books_read
from elastic.repository import ReaderESRepository, BookESRepository
from neo4j.jobs.create import JobCreateGraph

from pgvector.jobs import JobCreateTable, JobInsertTable
from spark.repository import ReaderSparkRepository, BookSparkRepository

samples_path = Path(__file__).parent.parent / "samples"


def gen_data(filename: str):
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


if __name__ == "__main__":
    jobs: deque = deque()
    with elastic_client(**settings.ES_CONFIG.model_dump(by_alias=True)) as es_client:
        jobs.append(JobCleanIndicies(es_client))
        reader_repo = ReaderESRepository(es_client)
        jobs.append(
            JobCreateIndex(
                reader_repo,
                gen_data(samples_path / "readers.jsonl"),
            )
        )
        book_repo = BookESRepository(es_client)
        jobs.append(
            JobCreateIndex(
                book_repo,
                gen_data(samples_path / "books.jsonl"),
            )
        )
        pipeline = Pipeline(jobs)
        pipeline.run()

        pprint(book_repo.get_by(**expired_books))
        pprint(reader_repo.get_by(**total_books_read))

        readers_response = reader_repo.get_by(size=25)
        readers = {
            int(hit["_id"]): hit["_source"] for hit in readers_response["hits"]["hits"]
        }

        books_reponse = book_repo.get_by(size=25)
        books = {
            int(hit["_id"]): hit["_source"] for hit in books_reponse["hits"]["hits"]
        }

    with neo4j_client(
        **settings.NEO4J_CONFIG.model_dump(by_alias=True)
    ) as graph_client:
        jobs.append(JobCreateGraph(graph_client, readers, books))
        pipeline = Pipeline(jobs)
        pipeline.run()
        try:
            result = graph_client.run(
                """
                MATCH (b:Book)<-[:ЧИТАЛ]-(r:Reader)
                RETURN b.Author AS Автор, COUNT(r) AS Число_читателей
                ORDER BY Число_читателей DESC
                LIMIT 1
                """
            )
            while result.forward():
                print(result.current)

        except Exception as exc:
            print(exc)

    # Сохранение в HDFS
    with spark_client() as spark:
        readers_values = [
            (reader_id, *reader.values()) for reader_id, reader in readers.items()
        ]
        books_values = []
        for book_id, book in books.items():
            for issue in book["issue"]:
                books_values.append(
                    (
                        book_id,
                        book["title"],
                        book["author"],
                        issue["reader_id"],
                        issue["return_date"],
                        issue["return_factual_date"],
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
            books_df, readers_df.read_book_id == books_df.id, "inner"
        ).filter(
            (col("issue.return_factual_date").isNull())
            | (col("issue.return_factual_date") > col("issue.return_date"))
        )

        # Показать результаты
        debtors_df.show()

    with pg_client(**settings.PG_CONFIG.model_dump(by_alias=True)) as pg_client:
        jobs.append(JobCreateTable(pg_client))
        jobs.append(JobInsertTable(pg_client, readers))
        pipeline = Pipeline(jobs)
        pipeline.run()

        with pg_client.cursor() as cur:
            # Поиск 3 ближайших документов
            cur.execute(
                """
                SELECT *
                FROM readers
                ORDER BY vector <-> (SELECT vector FROM readers LIMIT 1)
                LIMIT 3
                OFFSET 1;
                """
            )

        for row in cur:
            print(row)
