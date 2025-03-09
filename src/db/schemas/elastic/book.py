import datetime

from pydantic import BaseModel


class IssueSchema(BaseModel):
    reader_id: int
    issue_date: datetime.date
    return_date: datetime.date
    return_factual_date: datetime.date


class BookSchema(BaseModel):
    id: int | None = None
    title: str
    author: str
    publishing_house: list[str]
    year_issue: int
    language: str
    shelf: str
    issue: list[IssueSchema]
