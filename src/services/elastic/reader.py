from db.repositories.elastic.reader import ReaderElasticRepository
from services.elastic.base_service import BaseService


class ReaderElasticService(BaseService):
    def get_all(self, size):
        return ReaderElasticRepository(self.db).get_all(size=size)

    def get_total_books_read(self):
        return ReaderElasticRepository(self.db).get_total_books_read()

    def create(self, data):
        try:
            ReaderElasticRepository(self.db).create(
                document_id=data["id"], body=data["body"]
            )
        except Exception as exc:
            print(f"Create document error: {exc}")

    def delete(self, instance_id):
        ReaderElasticRepository(self.db).delete(instance_id)
