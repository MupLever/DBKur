from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Schemas для Читателей и Книг
ReadersSchema = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("registration_date", StringType(), False),
        StructField("fullname", StringType(), False),
        StructField("birthdate", StringType(), False),
        StructField("address", StringType(), False),
        StructField("e-mail", StringType(), False),
        StructField("education", StringType(), False),
    ]
)

BooksSchema = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("reader_id", IntegerType(), False),
        StructField("publishing_house", StringType(), False),
        StructField("year_issue", StringType(), False),
        StructField("language", StringType(), False),
        StructField("shelf", StringType(), False),
        StructField("return_date", DateType(), False),
        StructField("return_factual_date", DateType(), False),
    ]
)
