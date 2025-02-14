ANALYZER_NAME = "custom_analyzer"

analyzer = {
    "settings": {
        "analysis": {
            "filter": {
                "ru_stop": {"type": "stop", "stopwords": "_russian_"},
                "ru_stemmer": {"type": "stemmer", "language": "russian"},
            },
            "analyzer": {
                ANALYZER_NAME: {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "ru_stop", "ru_stemmer"],
                }
            },
        }
    }
}

reader_mappings = {
    "mappings": {
        "properties": {
            "registration_date": {"type": "date"},
            "fullname": {"type": "text", "analyzer": "standard"},
            "birthdate": {"type": "date"},
            "address": {"type": "text", "analyzer": ANALYZER_NAME},
            "e-mail": {"type": "keyword"},
            "education": {"type": "text", "analyzer": "standard"},
            "read_book_id": {
                "type": "nested",
                "properties": {"id": {"type": "keyword"}},
            },
            "reader_review": {
                "type": "nested",
                "properties": {"review": {"type": "text", "analyzer": ANALYZER_NAME}},
            },
        }
    },
    **analyzer,
}

book_mappings = {
    "mappings": {
        "properties": {
            "title": {"type": "text", "analyzer": "standard"},
            "author": {"type": "text", "analyzer": "standard"},
            "publishing_house": {"type": "text", "analyzer": ANALYZER_NAME},
            "year_issue": {"type": "date"},
            "language": {"type": "keyword"},
            "shelf": {"type": "keyword"},
            "issue": {
                "type": "nested",
                "properties": {
                    "reader_id": {"type": "keyword"},
                    "issue_date": {"type": "date"},
                    "return_date": {"type": "date"},
                    "return_factual_date": {"type": "date"},
                },
            },
        }
    },
    **analyzer,
}
