import contextlib
from typing import Any, Mapping

from elasticsearch import Elasticsearch

# https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-stop-tokenfilter.html
# https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/snowball/russian_stop.txt
# Конфигурация анализатора
ANALYZER_NAME = "custom_analyzer"
analyzer: dict[str, Any] = {
    "settings": {
        "analysis": {
            "filter": {
                "ru_stop": {"type": "stop", "stopwords": "_russian_"},
                # "ru_stemmer": {"type": "stemmer", "language": "russian"}, # https://snowballstem.org/algorithms/russian/stemmer.html
            },
            "analyzer": {
                ANALYZER_NAME: {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "ru_stop"],
                }
            },
        }
    }
}


class BaseElasticRepository:
    """Generic-репозиторий для работы с ES."""

    index: str
    mappings: dict[str, Any]

    def __init__(self, db: Elasticsearch) -> None:
        self.db = db

        with contextlib.suppress(Exception):
            db.indices.create(index=self.index, body=self.mappings)

    def get_all(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self.db.search(index=self.index, **kwargs)["hits"]["hits"]

    def create(self, *, document_id: str, body: Mapping[str, Any]) -> None:
        self.db.index(index=self.index, id=document_id, document=body)
        print(f"Added doc {document_id} to {self.index}")

    def delete(self, instance_id: str) -> None:
        self.db.delete(index=self.index, id=instance_id)
