from db.repositories.spark.base_repository import BaseSparkRepository
from db.schemas.spark.reader import ReaderSchema


class ReaderSparkRepository(BaseSparkRepository[ReaderSchema]):
    """"""

    index = "readers"
    schema = ReaderSchema
