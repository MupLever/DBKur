from typing import Any

from psycopg2._psycopg import connection
from sentence_transformers import SentenceTransformer

from db.schemas.elastic.reader import ReaderSchema


class InsertValuesScript:
    def __init__(self, db: connection, data: list[ReaderSchema]) -> None:
        self.db = db
        self.data = data

        # Загрузка модели для преобразования текста в векторы
        self.model = SentenceTransformer("all-MiniLM-L6-v2")

    def _transform_doc_to_vector(self) -> Any:
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
            for doc in self.data
        ]
        vectors = self.model.encode(texts).tolist()
        return vectors

    def run(self) -> None:
        vectors = self._transform_doc_to_vector()
        with self.db.cursor() as cur:
            # Сохранение векторов в базу данных
            for doc, vector in zip(self.data, vectors):
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
                        str(vector),
                    ),
                )
        # Сохранение изменений
        self.db.commit()
