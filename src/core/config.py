import os

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.mixins.model_mixin import LowerCaseMixin


class PostgresConfig(LowerCaseMixin):
    HOST: str = Field(default="localhost")
    PORT: int = Field(default=5432)
    DBNAME: str = Field(default="postgres")
    USER: str = Field(default="postgres")
    PASSWORD: str = Field(default="postgres")


class ElasticConfig(LowerCaseMixin):
    HOST: str = Field(default="localhost")
    PORT: int = Field(default=9200)


class Neo4jConfig(LowerCaseMixin):
    HOST: str = Field(default="localhost")
    PORT: int = Field(default=7687)
    USER: str = Field(default="neo4j", exclude=True)
    PASSWORD: str = Field(default="test", exclude=True)

    @computed_field
    @property
    def auth(self) -> tuple[str, ...]:
        return self.USER, self.PASSWORD


class HadoopConfig(LowerCaseMixin):
    HOST: str = Field(default="localhost")
    PORT: int = Field(default=9000)


class SparkConfig(LowerCaseMixin):
    HOST: str = Field(default="localhost")
    PORT: int = Field(default=7077)
    HADOOP_CONFIG: HadoopConfig = HadoopConfig()


class Settings(BaseSettings):
    PG_CONFIG: PostgresConfig = PostgresConfig()
    ES_CONFIG: ElasticConfig = ElasticConfig()
    NEO4J_CONFIG: Neo4jConfig = Neo4jConfig()
    SPARK_CONFIG: SparkConfig = SparkConfig()

    model_config = SettingsConfigDict(
        extra="ignore",
        env_file=os.environ.get("ENV_FILE", ".env"),
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
    )


settings = Settings()
