from typing import Any

from db.repositories.elastic.book import BookElasticRepository
from db.schemas.elastic.book import BookSchema
from services.elastic.base_service import BaseElasticService


class BookElasticService(BaseElasticService[BookElasticRepository, BookSchema]):
    repository = BookElasticRepository
    schema = BookSchema

    def get_expired_books(self) -> Any:
        return self.repository(self.db).get_expired_books()
