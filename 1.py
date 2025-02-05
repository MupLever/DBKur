import random
import json
from faker import Faker

# Использование библиотеки Faker для создания авторов и текста
fake = Faker('ru_RU')
# Список с названиями журналов
journal_names = [
    "Playboy",
    "Harvard Business Review",
    "Time",
    "Bloomberg",
    "The Guardian",
    "Digital Photographer",
    "The Atlantic",
    "Cosmopolitan",
    "Bloomberg",
    "The New Yorker",
    "Playboy",
    "Vogue"
]
# Списки с авторами и с случайной информацией
authors = [fake.name() for _ in range(100)]
author_info = [fake.text() for _ in range(100)]
# Генерация списка статей
articles = []
for i in range(25):
    article = {
        "index": "article",
        "doc_type": "article",
        "id": i,
        "body": {
            "id_article": i,
            "information_article": fake.text().replace('\n', ' '),
            "id_author": fake.uuid4(),
            "information_author": random.choice(authors),
            "fee": fake.pyint(),
            "name_journal": random.choice(journal_names),
                "id_issue": random.randint(0, 25)
}
}
articles.append(article)
# Генерация списка номеров журналов
journal_issues = []
for i in range(25):
    issue = {
        "index": "issue",
        "doc_type": "issue",
        "id": i,
        "body": {
            "name_journal": random.choice(journal_names),
            "year": fake.year(),
            "issue": fake.random_int(min=0, max=25),
            "annotation_article": [fake.text().replace('\n', ' ') for _ in
                                   range(4)]
        }
    }
journal_issues.append(issue)
# Сохранение списков в JSON-файлы
with open("articles.json", "w") as f:
    json.dump(articles, f, ensure_ascii=False, indent=4)
with open("journal_issues.json", "w") as f:
    json.dump(journal_issues, f, ensure_ascii=False, indent=4)
