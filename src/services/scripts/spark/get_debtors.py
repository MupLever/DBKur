from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from core.config import HadoopConfig
from services.spark.book import BookSparkService
from services.spark.reader import ReaderSparkService


class GetDebtorsScript:
    def __init__(self, db: SparkSession, config: HadoopConfig) -> None:
        self.db = db
        self.config = config

    def run(self) -> DataFrame:
        # self.db.sql(
        #     """
        #     SELECT r.*
        #     FROM readers r
        #     JOIN books b ON r.read_book_id = b.id
        #     WHERE b.issue.return_factual_date IS NULL
        #     OR b.issue.return_factual_date > b.issue.return_date;
        #     """
        # )
        readers_df = ReaderSparkService(self.db, self.config).get_all()
        books_df = BookSparkService(self.db, self.config).get_all()

        # Преобразуем данные для поиска задолженности
        debtors_df = readers_df.join(
            books_df, on=readers_df["read_book_id"] == books_df["id"], how="inner"
        ).filter(
            (col("issue.return_factual_date").isNull())
            | (col("issue.return_factual_date") > col("issue.return_date"))
        )

        return debtors_df
