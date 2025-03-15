from typing import Any

from db.repositories.elastic.reader import ReaderElasticRepository
from db.schemas.elastic.reader import ReaderSchema
from services.elastic.base_service import BaseElasticService


class ReaderElasticService(BaseElasticService[ReaderElasticRepository, ReaderSchema]):
    repository = ReaderElasticRepository
    schema = ReaderSchema

    def get_total_books_read(self) -> Any:
        return self.repository(self.db).get_total_books_read()
