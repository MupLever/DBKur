from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Schema для Читателей
ReaderSchema = StructType(
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
