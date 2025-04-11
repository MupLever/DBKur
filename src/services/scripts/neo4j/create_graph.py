from py2neo import Node, Relationship, Graph

from db.schemas.elastic.book import BookSchema
from db.schemas.elastic.reader import ReaderSchema


class CreateGraphScript:
    """Создание графа читателей, кинг и связей между ними."""

    def __init__(
        self, db: Graph, readers: list[ReaderSchema], books: list[BookSchema]
    ) -> None:
        self.db = db
        self.readers = readers
        self.books = books

    def run(self) -> None:
        """Создание графа читателей, кинг и связей между ними."""
        self.db.delete_all()
        reader_nodes = {}
        # Создаем узлы читателей
        for reader in self.readers:
            try:
                reader_node = Node(
                    "Reader",
                    Fullname=reader.fullname,
                    Birthdate=reader.birthdate.strftime("%Y-%m-%d"),
                    RegistrationDate=reader.registration_date.strftime("%Y-%m-%d"),
                    Education=reader.education,
                )
                self.db.create(reader_node)
                reader_nodes[reader.id] = reader_node
            except Exception as exc:
                print(exc)

        # Создаем узлы книг
        for book in self.books:
            try:
                book_node = Node(
                    "Book",
                    Title=book.title,
                    Author=book.author,
                    YearIssue=book.year_issue,
                )
                self.db.create(book_node)
            except Exception as exc:
                print(exc)
                continue

            # Создаем связи "Читал" между книгой и читателем
            for issue in book.issue:
                try:
                    reader_node = reader_nodes[issue.reader_id]
                    nodes_relationship = Relationship(reader_node, "ЧИТАЛ", book_node)
                    self.db.create(nodes_relationship)
                    print(f"Added: {reader_node} ЧИТАЛ {book_node}")
                except Exception as exc:
                    print(exc)
