class GetSimilarVectorsScript:
    def __init__(self, db):
        self.db = db

    def run(self):
        with self.db.cursor() as cur:
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
