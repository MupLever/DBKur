import json
from elasticsearch import Elasticsearch

# Параметры соединения с ES
client = Elasticsearch("http://localhost:9200")
# Названия документов статьи и номера журналов
articles = "articles.json"
issues = "journal_issues.json"
# Индексы

articles_id = "article"
issues_id = "issue"
# Конфигурация анализатора
analyzer_settings = {
    "settings": {
        "analysis": {
            "filter": {
                "ru_stop": {
                    "type": "stop",
                    "stopwords": "_russian_"
                },
                "ru_stemmer": {
                    "type": "stemmer",
                    "language": "russian"
                }
            },
            "analyzer": {
                "custom_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "ru_stop",
                        "ru_stemmer"
                    ]
                }
            }
        }
    }
}

# Определение маппинга для статей
mappings_articles = {
    "mappings": {
        "properties": {
            "id_article": {
                "type": "keyword"
            },
            "information_author": {
                "type": "text",
                    "analyzer": "custom_analyzer"
},
"id_author": {
    "type": "keyword"
},
"information_article": {
    "type": "text",
    "analyzer": "custom_analyzer"
},
"fee": {
    "type": "integer"
},
"name_journal": {
    "type": "text",
    "analyzer": "custom_analyzer",
    "fields": {
        "raw": {
            "type": "keyword"
        }
    }
},
"id_issue": {
    "type": "keyword"
}
}
}}
# Определение маппинга для номеров журналов
mappings_issues = {
    "mappings": {
        "properties": {
            "name_journal": {
                "type": "text",
                "analyzer": "custom_analyzer",
                "fields": {
                    "raw": {
                        "type": "keyword"
                    }
                }
            },
            "year": {
                "type": "integer"
            },
            "issue": {
                "type": "integer"
            },
            "annotation_article": {
                "type": "text",
                "analyzer": "custom_analyzer"
            }
        }
    }
}
print("Creating an index...")
# Если уже есть индексы, то их надо удалить
# Для статей
if client.indices.exists(index=articles_id):
    print("Recreate " + articles_id + " index")
    client.indices.delete(index=articles_id)
    client.indices.create(index=articles_id, body={**analyzer_settings,
                                                   **mappings_articles})
else:
    print("Create " + articles_id)
    client.indices.create(index=articles_id,
                          body={**analyzer_settings, **mappings_articles})
# Для номеров журналов
if client.indices.exists(index=issues_id):
    print("Recreate " + issues_id + " index")
    client.indices.delete(index=issues_id)
    client.indices.create(index=issues_id, body={**analyzer_settings,
                                                 **mappings_issues})
else:
    print("Create " + issues_id)
    client.indices.create(index=issues_id, body={**analyzer_settings,
                                                 **mappings_issues})
# Читаем документы
print("Reading documents...")
with open(articles, 'r') as file_data:
    dataStore = json.load(file_data)
for data in dataStore:
    try:
        client.index(index=data["index"],
                     id=data["id"],
                     body=data["body"]
                     )
    except Exception as e:
        print(e)
print("Document w/ articles read")
with open(issues, 'r') as file_data:
    dataStore = json.load(file_data)
for data in dataStore:
    try:
        client.index(index=data["index"],
                 id=data["id"],
                 body=data["body"]
                 )
    except Exception as e:
        print(e)
print("Document w/ issues read")
# Вывод, что индексирование прошло успешно
print("All documents indexed successfully")
