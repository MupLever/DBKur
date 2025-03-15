from db.repositories.spark.book import BookSparkRepository
from db.schemas.spark.book import BookSchema
from services.spark.base_service import BaseSparkService


class BookSparkService(BaseSparkService[BookSchema]):
    repository = BookSparkRepository
