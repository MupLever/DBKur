from db.repositories.elastic.base_repository import (
    BaseElasticRepository,
    ANALYZER_NAME,
    analyzer,
)
from db.schemas.elastic.book import BookSchema
from services.elastic.queries import expired_books

book_mappings = {
    "mappings": {
        "properties": {
            "title": {"type": "text", "analyzer": "standard"},
            "author": {"type": "text", "analyzer": "standard"},
            "publishing_house": {"type": "text", "analyzer": ANALYZER_NAME},
            "year_issue": {"type": "keyword"},
            "language": {"type": "keyword"},
            "shelf": {"type": "keyword"},
            "issue": {
                "type": "nested",
                "properties": {
                    "reader_id": {"type": "keyword"},
                    "issue_date": {"type": "keyword"},
                    "return_date": {"type": "keyword"},
                    "return_factual_date": {"type": "keyword"},
                },
            },
        }
    },
    **analyzer,
}


class BookElasticRepository(BaseElasticRepository):
    index = "books"
    mappings = book_mappings
    schema = BookSchema

    def get_expired_books(self):
        return self.db.search(index=self.index, **expired_books).body
