from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import findspark

findspark.init()
# Установление соединения с Eslaticsearch
client = Elasticsearch("http://localhost:9200")

# Обозначение индексов для работы с ES
indexArticles = "article"
indexIssues = "issue"
# Создангие Spark Session
sparkSession = SparkSession.builder.appName("csv").getOrCreate()
searchBody = {
    "size": 500,
    "query": {
        "match_all": {}
    }
}
# Articles
articles = client.search(index=indexArticles, query={"match_all": {}}, size
=10000)['hits']['hits']
# Issues
issues = client.search(index=indexIssues, query={"match_all": {}}, size=
10000)['hits']['hits']

# Schemes для Авторов, Номеров и Журналов
AuthorsSchema = StructType([
    StructField("id_author", StringType(), False),
    StructField("information_author", StringType(), False),
])
IssuesSchema = StructType([
    StructField("issue", IntegerType(), False),
    StructField("year", IntegerType(), False),
    StructField("name_journal", StringType(), False),
    StructField("id_author", StringType(), False)
])
JournalsSchema = StructType([
    StructField("name_journal", StringType(), False)
])
# Списки для каждой таблицы
AuthorTable = []
IssueTable = []
JournalTable = []

# Проход по каждой статье из таблицы Articles
for article in articles:
    # Заполнение таблицы авторов
    AuthorTable.append((
        article['_source']['id_author'],
        article['_source']['information_author'],
    ))
    # Проход по каждому номеру из таблицы Issues
for issue in issues:
    article_search = client.search(index=indexArticles, query={
        "match": {
            "id_issue": issue['_id']
        }
    }, size=1)['hits']['hits']
if article_search:
    article_search = article_search[0]['_source']['id_author']
else:
    artilcle_search = "none"
    # Заполнение таблицы номеров журналов
    IssueTable.append((
        int(issue['_source']['issue']),
        int(issue['_source']['year']),
        str(issue['_source']['name_journal']),
        article_search
    ))
# Заполнение таблицы журналов
JournalTable.append((
    str(issue['_source']['name_journal']),
))
# Создаем DateFrames для каждой таблицы
JournalTable = set(JournalTable)
AuthorTable = sparkSession.createDataFrame(AuthorTable, AuthorsSchema)
IssueTable = sparkSession.createDataFrame(IssueTable, IssuesSchema)
JournalTable = sparkSession.createDataFrame(JournalTable, JournalsSchema)

# CSV writting для каждой таблицы
AuthorTable.write.csv(path='hdfs://localhost:9000/authors.csv',
                      mode='overwrite', header=True)

IssueTable.write.csv(path='hdfs://localhost:9000/issues.csv', mode='overwrite',
                     header=True)
JournalTable.write.csv(path='hdfs://localhost:9000/journals.csv',
                       mode='overwrite', header=True)
# Вывод всех таблиц
AuthorTable.show()
IssueTable.show(25)
JournalTable.show()
# Остановить Spark Session
sparkSession.stop()
