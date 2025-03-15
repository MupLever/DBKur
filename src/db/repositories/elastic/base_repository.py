from typing import Any

from elasticsearch import Elasticsearch, BadRequestError

ANALYZER_NAME = "custom_analyzer"
analyzer: dict[str, Any] = {
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


class BaseElasticRepository:
    index: str
    mappings: dict[str, Any]

    def __init__(self, db: Elasticsearch) -> None:
        self.db = db

        try:
            db.indices.create(index=self.index, body=self.mappings)
        except BadRequestError:
            pass

    def get_all(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self.db.search(index=self.index, **kwargs).body["hits"]["hits"]

    def create(self, *, document_id: str, body: dict[str, Any]) -> None:
        self.db.index(index=self.index, id=document_id, body=body)
        print(f"Added doc {document_id} to {self.index}")

    def delete(self, instance_id: str) -> None:
        self.db.delete(index=self.index, id=instance_id)
