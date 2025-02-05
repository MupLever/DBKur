import findspark

findspark.init()
from pyspark.sql import SparkSession

# Создание Spark Session
sparkSession = SparkSession.builder.appName("SparkSelect").getOrCreate()
# Загрузка CSV файлов из HDFS
# Авторы
data = sparkSession.read.load("hdfs://localhost:9000/authors.csv", format="csv",
                              sep=",", inferSchema="true", header="true")
data.registerTempTable("authors")
# Номера журналов
data = sparkSession.read.load("hdfs://localhost:9000/issues.csv", format="csv",
                              sep=",", inferSchema="true", header="true")
data.registerTempTable("issues")
# Журналы
data = sparkSession.read.load("hdfs://localhost:9000/journals.csv",
                              format="csv",
                              sep=",", inferSchema="true", header="true")
data.registerTempTable("journals")
# Запрос для вывода числа авторов по каждому журналу
sparkSession.sql(
    """
    SELECT journals.name_journal, COUNT(DISTINCT authors.id_author) AS author_count
    FROM journals
    JOIN issues ON journals.name_journal = issues.name_journal
    JOIN authors ON issues.id_author = authors.id_author
    GROUP BY journals.name_journal
    """
).show()
input()
