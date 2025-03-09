class GetReadableWriterScript:
    def __init__(self, db):
        self.db = db

    def run(self):
        try:
            result = self.db.run(
                """
                MATCH (r:Reader)-[:ЧИТАЛ]->(b:Book)
                RETURN b.Author AS Автор, COUNT(r) AS Число_читателей
                ORDER BY Число_читателей DESC
                LIMIT 1
                """
            )
            while result.forward():
                print(result.current)

        except Exception as exc:
            print(exc)
