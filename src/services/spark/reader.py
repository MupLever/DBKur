from db.repositories.spark.reader import ReaderSparkRepository
from db.schemas.spark.book import BookSchema
from services.spark.base_service import BaseSparkService


class ReaderSparkService(BaseSparkService[BookSchema]):
    repository = ReaderSparkRepository
