from typing import Any

from db.repositories.elastic.base_repository import (
    BaseElasticRepository,
    ANALYZER_NAME,
    analyzer,
)

# Схема маппинга книги
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
                    "issue_date": {"type": "date", "format": "yyyy-MM-dd"},
                    "return_date": {"type": "date", "format": "yyyy-MM-dd"},
                    "return_factual_date": {"type": "date", "format": "yyyy-MM-dd"},
                },
            },
        }
    },
    **analyzer,
}
# Запрос на получение книг с просроченной датой в группировке по дате выдачи
expired_books: dict[str, Any] = {
    "size": 0,
    "aggs": {
        "nested": {
            "nested": {"path": "issue"},
            "aggs": {
                "books_by_issue_date": {
                    "terms": {"field": "issue.issue_date"},
                    "aggs": {
                        "filtered_issues": {
                            "filter": {
                                "script": {
                                    "script": {
                                        "source": "doc['issue.return_date'].value.isBefore(doc['issue.return_factual_date'].value)",
                                        "lang": "painless",
                                    }
                                }
                            },
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
    """Репозиторий Книги."""

    index = "books"
    mappings = book_mappings

    def get_expired_books(self) -> Any:
        return self.db.search(index=self.index, **expired_books)
