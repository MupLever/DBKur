import datetime

from pydantic import BaseModel, EmailStr


class ReaderSchema(BaseModel):
    id: int | None = None
    registration_date: datetime.date
    fullname: str
    birthdate: datetime.date
    address: str
    email: EmailStr
    education: str
    read_book_id: list[int]
    reader_review: list[str]
