expired_books = {
    "size": 0,
    "aggs": {
        "books_by_issue_date": {
            "terms": {"field": "issue.issue_date"},
            "aggs": {
                "overdue_books": {
                    "filter": {
                        "bool": {
                            "should": [
                                {"range": {"issue.return_date": {"lt": "now"}}},
                                {"exists": {"field": "issue.return_factual_date"}},
                            ]
                        }
                    },
                    "aggs": {"count": {"value_count": {"field": "issue.reader_id"}}},
                }
            },
        }
    },
}

total_books_read = {
    "size": 0,
    "aggs": {"total_books_read": {"value_count": {"field": "read_book_id"}}},
}
