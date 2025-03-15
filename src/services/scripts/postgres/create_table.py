from psycopg2._psycopg import connection


class CreateTableScript:
    def __init__(self, db: connection) -> None:
        self.db = db

    def run(self) -> None:
        with self.db.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cur.execute("DROP TABLE IF EXISTS readers;")
            cur.execute(
                """
                CREATE TABLE readers (
                    id SERIAL PRIMARY KEY,
                    registration_date DATE,
                    fullname VARCHAR(100),
                    address VARCHAR(255),
                    email VARCHAR(50),
                    birthdate DATE,
                    education TEXT,
                    embedding VECTOR(384)
                );
                """
            )

        # Сохранение изменений
        self.db.commit()
