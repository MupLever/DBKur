from db.repositories.spark.reader import ReaderSparkRepository
from db.schemas.spark.reader import ReaderSchema
from services.spark.base_service import BaseSparkService


class ReaderSparkService(BaseSparkService[ReaderSchema]):
    repository = ReaderSparkRepository
