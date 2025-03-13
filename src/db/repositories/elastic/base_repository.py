from elasticsearch import Elasticsearch, BadRequestError

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
