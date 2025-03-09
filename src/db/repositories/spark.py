from pyspark.sql import SparkSession, DataFrame

from core.config import SparkConfig
from db.schemas.spark.book import BookSchema
from db.schemas.spark.reader import ReaderSchema


class BaseSparkRepository:
    index = None
    schema = None

    def __init__(self, client: SparkSession, config: SparkConfig) -> None:
        self.client = client
        self.config = config

    def create(self, values) -> None:
        df = self.client.createDataFrame(values, self.schema)
        try:
            existing_df = self.client.read.csv(
                path="hdfs://{host}:{port}/{index}.csv".format(
                    host=self.config.HADOOP_CONFIG.HOST,
                    port=self.config.HADOOP_CONFIG.PORT,
                    index=self.index,
                ),
                header=True,
                inferSchema=True,
            )
            df = existing_df.union(df)
        except Exception as exc:
            print(f"File not exists: {exc}")

        df.write.csv(
            path="hdfs://{host}:{port}/{index}.csv".format(
                host=self.config.HADOOP_CONFIG.HOST,
                port=self.config.HADOOP_CONFIG.PORT,
                index=self.index,
            ),
            mode="overwrite",
            header=True,
        )

    def get_all(self) -> DataFrame:
        return self.client.read.csv(
            path="hdfs://{host}:{port}/{index}.csv".format(
                host=self.config.HADOOP_CONFIG.HOST,
                port=self.config.HADOOP_CONFIG.PORT,
                index=self.index,
            ),
            header=True,
            inferSchema=True,
        )


class ReaderSparkRepository(BaseSparkRepository):
    """"""

    index = "readers"
    schema = ReaderSchema


class BookSparkRepository(BaseSparkRepository):
    """"""

    index = "books"
    schema = BookSchema
