from typing import Generator

from src.elastic.repository import BaseElasticSearchRepository
from src.common.job import BaseJob


class JobCreateIndex(BaseJob):
    def __init__(self, repository: BaseElasticSearchRepository, data_store: Generator):
        self.repository = repository
        self.data_store = data_store

    def run(self):
        for data in self.data_store:
            try:
                self.repository.create(document_id=data["id"], body=data["body"])
            except Exception as exc:
                print(f"Create document error: {exc}")
