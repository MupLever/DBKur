from elasticsearch import Elasticsearch

from src.common.job import BaseJob


class JobCleanIndicies(BaseJob):
    def __init__(self, client: Elasticsearch):
        self.client = client

    def run(self):
        for index in self.client.cat.indices(format="json"):
            index_name = index["index"]
            self.client.indices.delete(index=index_name, ignore_unavailable=True)
