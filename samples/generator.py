import random
import json
from faker import Faker
from datetime import date, datetime

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

# Авторы
authors = [
    "Л.Н. Толстой",
    "А.С. Пушкин",
    "Ф.М. Достоевский",
    "А.П. Чехов",
]

# Генерация списка писателей
start_date = date(1950, 1, 1)
end_date = date.today()
readers = {}
for i in range(1, 26):
    birthdate = fake.date_between_dates(start_date, end_date)
    reader = {
        "index": "readers",
        "doc_type": "reader",
        "id": str(i),
        "body": {
            "registration_date": fake.date_between_dates(birthdate, end_date).strftime(
                "%Y-%m-%d"
            ),
            "fullname": f"{fake.last_name_male()} {fake.first_name_male()} {fake.random_element(patronymics)}",
            "birthdate": birthdate.strftime("%Y-%m-%d"),
            "address": fake.address(),
            "email": fake.email(),
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
            "read_book_id": [],
            "reader_review": [
                "Книга очень понравилась, захватывающий сюжет!",
                "Интересные персонажи и неожиданные повороты.",
            ],
        },
    }

    readers[i] = reader

# Генерация списка книг
books = []
for book_id in range(1, 26):
    issues = []
    for reader_id in set([random.randint(1, 25) for _ in range(random.randint(1, 25))]):
        readers[reader_id]["body"]["read_book_id"].append(book_id)
        registration_date = datetime.strptime(
            readers[reader_id]["body"]["registration_date"], "%Y-%m-%d"
        )
        issue_date = fake.date_between_dates(registration_date, end_date)
        issue = {
            "reader_id": reader_id,
            "issue_date": issue_date.strftime("%Y-%m-%d"),
            "return_date": fake.date_between_dates(issue_date, end_date).strftime(
                "%Y-%m-%d"
            ),
            "return_factual_date": fake.date_between_dates(
                issue_date, end_date
            ).strftime("%Y-%m-%d"),
        }
        issues.append(issue)

    book = {
        "index": "books",
        "doc_type": "book",
        "id": str(book_id),
        "body": {
            "title": fake.text(),
            "author": fake.random_element(authors),
            "publishing_house": ["Эксмо", "АСТ"],
            "year_issue": int(fake.year()),
            "language": fake.language_name(),
            "shelf": "Классика",
            "issue": issues,
        },
    }

    books.append(book)

# Сохранение списков в JSONL-файлы
with open("readers.jsonl", "w", encoding="utf-8") as f:
    for reader in readers.values():
        f.write(json.dumps(reader, ensure_ascii=False) + "\n")

with open("books.jsonl", "w", encoding="utf-8") as f:
    for book in books:
        f.write(json.dumps(book, ensure_ascii=False) + "\n")
