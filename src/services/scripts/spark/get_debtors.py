from pyspark.sql import SparkSession, DataFrame

from core.config import HadoopConfig
from services.spark.book import BookSparkService
from services.spark.reader import ReaderSparkService


class GetDebtorsScript:
    """Поиск должников, вернувших книги после дата возврата."""

    def __init__(self, db: SparkSession, config: HadoopConfig) -> None:
        self.db = db
        self.config = config

    def run(self) -> DataFrame:
        """Поиск должников, вернувших книги после дата возврата."""
        readers_df = ReaderSparkService(self.db, self.config).get_all()
        books_df = BookSparkService(self.db, self.config).get_all()

        # Преобразуем данные для поиска должников
        debtors_df = (
            readers_df.join(
                books_df, on=readers_df["id"] == books_df["reader_id"], how="inner"
            )
            .where(books_df["return_factual_date"] > books_df["return_date"])
            .select(readers_df["id"], *readers_df.columns[1:])
            .dropDuplicates(["id"])
        )

        return debtors_df
