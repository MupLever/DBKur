from psycopg2._psycopg import connection


class CreateTableScript:
    def __init__(self, db: connection) -> None:
        self.db = db

    def run(self) -> None:
        with self.db.cursor() as cur:
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
        self.db.commit()
