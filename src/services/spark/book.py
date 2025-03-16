from db.repositories.spark.book import BookSparkRepository
from services.spark.base_service import BaseSparkService


class BookSparkService(BaseSparkService):
    repository = BookSparkRepository
