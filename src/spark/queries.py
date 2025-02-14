debtors_query = """
    SELECT r.*
    FROM readers r
    JOIN books b ON r.read_book_id = b.id
    WHERE b.issue.return_factual_date IS NULL
    OR b.issue.return_factual_date > b.issue.return_date;
    """
