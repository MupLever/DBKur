expired_books = {
    "size": 0,
    "aggs": {
        "by_issue_date": {
            "date_histogram": {
                "field": "issue.issue_date",
                "calendar_interval": "month",
            },
            "aggs": {
                "expired": {
                    "filter": {"range": {"issue.return_date": {"lt": "now"}}},
                    "aggs": {"books_amount": {"value_count": {"field": "id"}}},
                }
            },
        }
    },
}

total_books_read = {
    "size": 0,
    "aggs": {"total_books_read": {"value_count": {"field": "read_book_id"}}},
}
