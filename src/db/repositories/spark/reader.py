from db.repositories.spark.base_repository import BaseSparkRepository
from db.schemas.spark.reader import ReaderSchema


class ReaderSparkRepository(BaseSparkRepository):
    """"""

    index = "readers"
    schema = ReaderSchema
