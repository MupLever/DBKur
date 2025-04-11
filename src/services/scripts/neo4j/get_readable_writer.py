from py2neo import Graph


class GetReadableWriterScript:
    """Поиск самого читаемого писателя."""

    def __init__(self, db: Graph) -> None:
        self.db = db

    def run(self) -> None:
        """Поиск самого читаемого писателя."""
        try:
            result = self.db.run(
                """
                MATCH (r:Reader)-[:ЧИТАЛ]->(b:Book)
                RETURN b.Author AS author, SIZE(COLLECT(DISTINCT r)) AS readers_count
                ORDER BY readers_count DESC
                LIMIT 1
                """
            )
            while result.forward():
                print(result.current)

        except Exception as exc:
            print(exc)
