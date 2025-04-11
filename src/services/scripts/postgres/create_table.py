import json
from typing import Any

from psycopg2._psycopg import connection
from sentence_transformers import SentenceTransformer

from db.schemas.elastic.reader import ReaderSchema


class CreateTableScript:
    """Создание и заполнение таблицы Читателей."""

    def __init__(self, db: connection, readers: list[ReaderSchema]) -> None:
        self.db = db
        self.readers = readers

        # Загрузка модели для преобразования текста в векторы
        self.model = SentenceTransformer("all-MiniLM-L6-v2")

    def run(self) -> None:
        """Создание и заполнение таблицы Читателей."""
        vectors = self._transform_reader_to_vector()

        with self.db.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cur.execute("DROP TABLE IF EXISTS readers;")
            # Создание схемы таблицы `Читатель`
            cur.execute(
                f"""
                CREATE TABLE readers (
                    id SERIAL PRIMARY KEY,
                    registration_date DATE,
                    fullname VARCHAR(100),
                    address VARCHAR(255),
                    email VARCHAR(50),
                    birthdate DATE,
                    education TEXT,
                    embedding VECTOR({len(vectors[0])})
                );
                """
            )

            # Сохранение данных в таблицу
            for doc, vector in zip(self.readers, vectors):
                cur.execute(
                    """
                    INSERT INTO readers (registration_date, fullname, address, email, birthdate, education, embedding)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        doc.registration_date,
                        doc.fullname,
                        doc.address,
                        doc.email,
                        doc.birthdate,
                        doc.education,
                        json.dumps(vector),
                    ),
                )
        # Фиксирование изменений
        self.db.commit()

    def _transform_reader_to_vector(self) -> Any:
        """Преобразование документов в векторы"""
        texts = [
            "{registration_date} {fullname} {address} {email} {birthdate} {education}".format(
                registration_date=doc.registration_date,
                fullname=doc.fullname,
                address=doc.address,
                email=doc.email,
                birthdate=doc.birthdate,
                education=doc.education,
            )
            for doc in self.readers
        ]
        vectors = self.model.encode(texts).tolist()

        return vectors
