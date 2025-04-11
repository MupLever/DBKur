from typing import TypeVar, Any, Generic

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from core.config import HadoopConfig
from db.repositories.spark.base_repository import BaseSparkRepository

Schema = TypeVar("Schema", bound=StructType)


class BaseSparkService(Generic[Schema]):
    """Generic-класс сервиса для работы со Spark."""

    repository: type[BaseSparkRepository[Schema]]

    def __init__(self, db: SparkSession, config: HadoopConfig) -> None:
        self.db = db
        self.config = config

    def bulk_insert(self, book_values: list[Any]) -> None:
        self.repository(self.db, self.config).bulk_insert(book_values)

    def get_all(self) -> DataFrame:
        return self.repository(self.db, self.config).get_all()
