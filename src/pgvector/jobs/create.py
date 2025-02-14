from src.common.job import BaseJob


class JobCreateTable(BaseJob):
    """Интерфейс для реализации Job'ов"""

    def __init__(self, client):
        self.client = client

    def run(self):
        with self.client.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS readers;")
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cur.execute(
                """
                CREATE TABLE readers (
                    id SERIAL PRIMARY KEY,
                    registration_date DATE,
                    fullname TEXT,
                    birthdate DATE,
                    education TEXT,
                    vector VECTOR(заменить)
                );
                """
            )

        # Сохранение изменений
        self.client.commit()
