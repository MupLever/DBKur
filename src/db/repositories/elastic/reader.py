from typing import Any

from db.repositories.elastic.base_repository import (
    BaseElasticRepository,
    ANALYZER_NAME,
    analyzer,
)
from db.schemas.elastic.reader import ReaderSchema


reader_mappings: dict[str, Any] = {
    "mappings": {
        "properties": {
            "registration_date": {"type": "date"},
            "fullname": {"type": "text", "analyzer": "standard"},
            "birthdate": {"type": "date"},
            "address": {"type": "text", "analyzer": ANALYZER_NAME},
            "email": {"type": "keyword"},
            "education": {"type": "text", "analyzer": "standard"},
            "read_book_id": {
                "type": "keyword",
            },
            "reader_review": {
                "type": "text",
                "analyzer": ANALYZER_NAME,
            },
        }
    },
    **analyzer,
}

total_books_read: dict[str, Any] = {
    "size": 0,
    "aggs": {"total_books_read": {"value_count": {"field": "read_book_id"}}},
}


class ReaderElasticRepository(BaseElasticRepository):
    index = "readers"
    mappings = reader_mappings
    schema = ReaderSchema

    def get_total_books_read(self) -> Any:
        return self.db.search(index=self.index, **total_books_read).body
