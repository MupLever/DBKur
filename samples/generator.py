import random
import json
from faker import Faker

# Использование библиотеки Faker для создания читателей и книг
fake = Faker("ru_RU")

# Список с отчествами
patronymics = [
    "Александрович",
    "Иванович",
    "Васильевич",
    "Ильич",
    "Павлович",
    "Николаевич",
    "Викторович",
]
authors = [
    "Л.Н. Толстой",
    "А.С. Пушкин",
    "Ф.М. Достоевский",
    "А.Н. Чехов",
]
# Генерация списка писателей
readers = []
for i in range(1, 26):
    reader = {
        "index": "readers",
        "doc_type": "reader",
        "id": i,
        "body": {
            "registration_date": fake.date(pattern="%Y-%m-%d"),
            "fullname": f"{fake.last_name_male()} {fake.first_name_male()} {fake.random_element(patronymics)}",
            "birthdate": fake.date(pattern="%Y-%m-%d"),
            "address": fake.address(),
            "e-mail": fake.email(),
            "education": fake.random_element(
                [
                    "Дошкольное",
                    "Начальное общее",
                    "Основное общее",
                    "Среднее общее",
                    "Среднее профессиональное",
                    "Высшее",
                ]
            ),
            "read_book_id": [
                random.randint(1, 25) for _ in range(random.randint(1, 25))
            ],
            "reader_review": [
                "Книга очень понравилась, захватывающий сюжет!",
                "Интересные персонажи и неожиданные повороты.",
            ],
        },
    }

    readers.append(reader)

# Генерация списка книг
books = []
for i in range(1, 26):
    book = {
        "index": "books",
        "doc_type": "book",
        "id": i,
        "body": {
            "title": fake.text(),
            "author": fake.random_element(authors),
            "publishing_house": ["Эксмо", "АСТ"],
            "year_issue": fake.year(),
            "language": fake.language_name(),
            "shelf": "Классика",
            "issue": [
                {
                    "reader_id": random.randint(1, 25),
                    "issue_date": fake.date(pattern="%Y-%m-%d"),
                    "return_date": fake.date(pattern="%Y-%m-%d"),
                    "return_factual_date": fake.date(pattern="%Y-%m-%d"),
                }
            ],
        },
    }

    books.append(book)

# Сохранение списков в JL-файлы
with open("readers.jsonl", "w", encoding="utf-8") as f:
    for reader in readers:
        f.write(json.dumps(reader, ensure_ascii=False) + "\n")

with open("books.jsonl", "w", encoding="utf-8") as f:
    for book in books:
        f.write(json.dumps(book, ensure_ascii=False) + "\n")
