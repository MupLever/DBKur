from pyspark.sql import SparkSession, DataFrame

from core.config import HadoopConfig


class BaseSparkService:
    repository = None

    def __init__(self, db: SparkSession, config: HadoopConfig) -> None:
        self.db = db
        self.config = config

    def bulk_insert(self, book_values: list) -> None:
        self.repository(self.db, self.config).bulk_insert(book_values)

    def get_all(self) -> DataFrame:
        return self.repository(self.db, self.config).get_all()
