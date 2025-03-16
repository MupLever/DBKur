from db.repositories.spark.reader import ReaderSparkRepository
from services.spark.base_service import BaseSparkService


class ReaderSparkService(BaseSparkService):
    repository = ReaderSparkRepository
