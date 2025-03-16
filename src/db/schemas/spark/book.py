from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Schema для Книг
BookSchema = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("author", StringType(), False),
        StructField("publishing_house", StringType(), False),
        StructField("year_issue", StringType(), False),
        StructField("language", StringType(), False),
        StructField("shelf", StringType(), False),
        StructField("reader_id", IntegerType(), False),
        StructField("issue_date", DateType(), False),
        StructField("return_date", DateType(), False),
        StructField("return_factual_date", DateType(), False),
    ]
)
