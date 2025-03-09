from db.repositories.elastic import BookElasticRepository
from mixins.base_service import BaseService


class BookElasticService(BaseService):
    def get_all(self, size):
        return BookElasticRepository(self.db).get_all(size=size)

    def get_expired_books(self):
        return BookElasticRepository(self.db).get_expired_books()

    def create(self, data):
        try:
            BookElasticRepository(self.db).create(
                document_id=data["id"], body=data["body"]
            )
        except Exception as exc:
            print(f"Create document error: {exc}")

    def delete(self, instance_id):
        BookElasticRepository(self.db).delete(instance_id)
