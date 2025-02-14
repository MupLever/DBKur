from elasticsearch import Elasticsearch, BadRequestError

from src.elastic.schemas import book_mappings, reader_mappings


class BaseElasticSearchRepository:
    index = None
    mappings = None

    def __init__(self, client: Elasticsearch):
        self.client = client

        try:
            client.indices.create(index=self.index, body=self.mappings)
        except BadRequestError:
            print(f"{self.index} already exists")

    def create(self, *, document_id: str, body: dict):
        return self.client.index(index=self.index, id=document_id, body=body)

    def get_by(self, **kwargs):
        return self.client.search(index=self.index, **kwargs).body


class ReaderESRepository(BaseElasticSearchRepository):
    index = "readers"
    mappings = reader_mappings


class BookESRepository(BaseElasticSearchRepository):
    index = "books"
    mappings = book_mappings
