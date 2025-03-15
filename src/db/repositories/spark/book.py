from db.repositories.spark.base_repository import BaseSparkRepository
from db.schemas.spark.book import BookSchema


class BookSparkRepository(BaseSparkRepository[BookSchema]):
    """"""

    index = "books"
    schema = BookSchema
