from elasticsearch import Elasticsearch, BadRequestError

from db.schemas.elastic.book import BookSchema
from db.schemas.elastic.reader import ReaderSchema
from services.elastic.queries import expired_books, total_books_read

ANALYZER_NAME = "custom_analyzer"

analyzer = {
    "settings": {
        "analysis": {
            "filter": {
                "ru_stop": {"type": "stop", "stopwords": "_russian_"},
                "ru_stemmer": {"type": "stemmer", "language": "russian"},
            },
            "analyzer": {
                ANALYZER_NAME: {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "ru_stop", "ru_stemmer"],
                }
            },
        }
    }
}

reader_mappings = {
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


class BaseElasticRepository:
    index = None
    mappings = None
    schema = None

    def __init__(self, db: Elasticsearch):
        self.db = db

        try:
            db.indices.create(index=self.index, body=self.mappings)
        except BadRequestError:
            pass

    def get_all(self, **kwargs):
        return [
            self.schema(id=data["_id"], **data["_source"])
            for data in self.db.search(index=self.index, **kwargs).body["hits"]["hits"]
        ]

    def create(self, *, document_id: str, body: dict):
        self.db.index(index=self.index, id=document_id, body=body)
        print(f"Added doc {document_id} to {self.index}")

    def delete(self, instance_id):
        self.db.delete(index=self.index, id=instance_id)


class ReaderElasticRepository(BaseElasticRepository):
    index = "readers"
    mappings = reader_mappings
    schema = ReaderSchema

    def get_total_books_read(self):
        return self.db.search(index=self.index, **total_books_read).body


class BookElasticRepository(BaseElasticRepository):
    index = "books"
    mappings = book_mappings
    schema = BookSchema

    def get_expired_books(self):
        return self.db.search(index=self.index, **expired_books).body
