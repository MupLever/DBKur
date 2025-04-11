from psycopg2._psycopg import connection


class GetSimilarVectorsScript:
    """Поиск трех похожих читателей."""

    def __init__(self, db: connection) -> None:
        self.db = db

    def run(self) -> None:
        """Поиск трех похожих читателей."""
        with self.db.cursor() as cur:
            # Поиск 3 ближайших документов
            cur.execute(
                """
                SELECT *
                FROM readers
                ORDER BY embedding <-> (SELECT embedding FROM readers LIMIT 1)
                LIMIT 3
                OFFSET 1;
                """
            )

            for row in cur:
                print(row)
