from typing import TypeVar, Generic, Any, Iterable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from core.config import HadoopConfig

Schema = TypeVar("Schema", bound=StructType)


class BaseSparkRepository(Generic[Schema]):
    index: str
    schema: type[Schema]

    def __init__(self, client: SparkSession, config: HadoopConfig) -> None:
        self.client = client
        self.config = config

    def bulk_insert(self, values: Iterable[Any]) -> None:
        df = self.client.createDataFrame(values, self.schema)
        try:
            existing_df = self.client.read.csv(
                path="hdfs://{host}:{port}/{index}.csv".format(
                    host=self.config.HOST,
                    port=self.config.PORT,
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
                host=self.config.HOST,
                port=self.config.PORT,
                index=self.index,
            ),
            mode="overwrite",
            header=True,
        )

    def get_all(self) -> DataFrame:
        return self.client.read.csv(
            path="hdfs://{host}:{port}/{index}.csv".format(
                host=self.config.HOST,
                port=self.config.PORT,
                index=self.index,
            ),
            header=True,
            inferSchema=True,
        )
