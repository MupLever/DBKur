from py2neo import Node, Relationship

from src.common.job import BaseJob


class JobCreateGraph(BaseJob):
    def __init__(self, client, readers, books):
        self.client = client
        self.readers = readers
        self.books = books

    def run(self):
        self.client.delete_all()
        reader_nodes = {}
        # Создаем узлы читателей
        for reader_id, reader in self.readers.items():
            try:
                reader_node = Node(
                    "Reader",
                    Fullname=reader["fullname"],
                    Birthdate=reader["birthdate"],
                    RegistrationDate=reader["registration_date"],
                    Education=reader["education"],
                )
                self.client.create(reader_node)
                reader_nodes[reader_id] = reader_node
            except Exception as exc:
                print(exc)

        # Создаем узлы книг
        for book in self.books.values():
            try:
                book_node = Node(
                    "Book",
                    Title=book["title"],
                    Author=book["author"],
                    YearIssue=book["year_issue"],
                )
                self.client.create(book_node)
            except Exception as exc:
                print(exc)
                continue

            # Создаем связи "Читал" между книгой и читателем
            for extradition in book["issue"]:
                reader_id = extradition["reader_id"]
                try:
                    reader_node = reader_nodes[reader_id]
                    nodes_relationship = Relationship(reader_node, "ЧИТАЛ", book_node)
                    self.client.create(nodes_relationship)
                    print(f"Added: {reader_node} ЧИТАЛ {book_node}")
                except Exception as exc:
                    print(exc)
