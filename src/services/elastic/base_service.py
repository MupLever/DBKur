from typing import TypeVar, Generic, Any, Mapping

from elasticsearch import Elasticsearch
from pydantic import BaseModel

from db.repositories.elastic.base_repository import BaseElasticRepository

Repository = TypeVar("Repository", bound=BaseElasticRepository)
Schema = TypeVar("Schema", bound=BaseModel)


class BaseElasticService(Generic[Repository, Schema]):
    repository: type[Repository]
    schema: type[Schema]

    def __init__(self, db: Elasticsearch) -> None:
        self.db = db

    def get_all(self, size: int) -> list[Schema]:
        return [
            self.schema(id=data["_id"], **data["_source"])
            for data in self.repository(self.db).get_all(size=size)
        ]

    def create(self, data: Mapping[str, Any]) -> None:
        try:
            self.repository(self.db).create(document_id=data["id"], body=data["body"])
        except Exception as exc:
            print(f"Create document error: {exc}")

    def delete(self, instance_id: str) -> None:
        self.repository(self.db).delete(instance_id)
