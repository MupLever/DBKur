from elasticsearch import Elasticsearch
from py2neo import Graph, Node, Relationship, NodeMatcher
from tqdm import tqdm

# Установление соединения с ES
client = Elasticsearch("http://localhost:9200")
# Обозначение индексов
indexArticle = "article"
indexIssue = "issue"
# Установление соединения с neo4j
graph_db = Graph("bolt://localhost:7687", auth=("neo4j", "ubuntu22"))
# Чистка всех предыдущих таблиц
graph_db.delete_all()

# Выгрузка из ES документов по индексам
articles = client.search(index=indexArticle, size=1000)
issues = client.search(index=indexIssue, size=1000)
# Проход по статьям для создания нодов "Article"
for article in tqdm(articles['hits']['hits']):
    try:
        # Заполнение ArticleNode, который состоит из:
        # ---ID Статьи---
        # ---ID автора---
        # ---Информация об авторе---
        # ---Номер журнала---
        # ---Название журнала---
        # ---Гонорар---
        articleNode = Node(
            "article",
            ArticleId=article['_source']['id_article'],
            AuthorId=article['_source']['id_author'],
            InformationAuthor=article['_source']['information_author'],
            Number=article['_source']['id_issue'],
            Name=article['_source']['name_journal'],
            Fee=article['_source']['fee']
        )
        # Создание ArticleNode
        graph_db.create(articleNode)
        # Если появилась ошибка, вывести ее и продолжить
    except Exception as e:
        print(e)
        continue
    # Проход по номерам журналов для создания нодов "Issue"
    for issue in tqdm(issues['hits']['hits']):
        try:
            # Заполнение IssueNode, который состоит из
            # ---ID номера журнала---
            # ---Название журнала---
            # ---Год публикации номера---
            # ---Номер журнала---
            issueNode = Node(
                "issue",
                IssueId=int(issue["_id"]),
                NameJournal=issue['_source']['name_journal'],
                Year=int(issue['_source']['year']),
                Issue=issue['_source']['issue'])
            # Создание IssueNode
            graph_db.create(issueNode)
            # Если есть ошибка, вывести ее и продолжить
        except Exception as e:
            print(e)
            continue
        # Создание отношение Статья - "Помещена" - Номер журнала
        for article in tqdm(articles['hits']['hits']):
            try:
                # Находим узел статьи
                article_node = NodeMatcher(graph_db).match("article",
                                                           Number=article['_source']['id_issue']).first()
                if article_node is None:
                    continue
                # Находим узел номера журнала, который соотвествует номеру журнала из статьи
                issue_node = NodeMatcher(graph_db).match("issue",
                                                         IssueId=article['_source']['id_issue']).first()
                if issue_node is None:
                    continue
                # Если есть такая статья и есть номер журнала, то создаем отношение
                "помещена"
                placed_relationship = Relationship(article_node, "located", issue_node)
                graph_db.create(placed_relationship)
                # Если ошибка, вывести и продолжить
            except Exception as e:
                print(e)
                continue
