from typing import Any

from db.repositories.elastic.base_repository import (
    BaseElasticRepository,
    ANALYZER_NAME,
    analyzer,
)
from db.schemas.elastic.book import BookSchema

book_mappings: dict[str, Any] = {
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
expired_books: dict[str, Any] = {
    "size": 0,
    "aggs": {
        "nested": {
            "nested": {"path": "issue"},
            "aggs": {
                "filtered_issues": {
                    "filter": {
                        "range": {
                            "issue.return_date": {"lt": "issue.return_factual_date"}
                        }
                    },
                    "aggs": {
                        "books_by_issue_date": {
                            "terms": {"field": "issue.issue_date"},
                            "aggs": {
                                "count": {"value_count": {"field": "issue.reader_id"}}
                            },
                        }
                    },
                }
            },
        }
    },
}


class BookElasticRepository(BaseElasticRepository):
    index = "books"
    mappings = book_mappings
    schema = BookSchema

    def get_expired_books(self) -> Any:
        return self.db.search(index=self.index, **expired_books).body
