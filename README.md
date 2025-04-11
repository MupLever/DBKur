# Сourse Database

### Elasticsearch.

Типы документов (json)

Читатель:

```
{
    index, 
    doc_type, 
    id, 
    body: {
        дата_регистрации, 
        ФИО, 
        дата_рождения, 
        адрес*, 
        e-mail, 
        образование,
        [id_прочитанной_книги],
        [отзыв_читателя*]
    }
}
```

Книга:

```
{
    index,
    doc_type,
    id,
    body: {
        название,
        автор,
        издательство*,
        год_издания,
        язык,
        полка,
        выдача: [
            {
                id_читателя,
                дата_выдачи,
                дата_возврата,
                дата_возврата_фактическая
            }
        ]
    }
}
```

Примечание. Квадратные скобки [] обозначает тег (может быть несколько значений)

**Требование к анализатору**:

1. поля, отмеченные *, разделить на слова
2. убрать пунктуацию с помощью токенизатора standard (русский)
3. перевести все токены в нижний регистр
4. убрать токены, находящиеся в списке стоп-слов.

**Запросы с вложенной агрегацией**:

- разбить книги по дате выдачи, для каждой группы определить число книг с просроченной датой возврата,
- определить общее число прочитанных книг.

### Neo4j.

1. По данным из Elasticsearch заполнить графовую базу данных «Читатель (дата_регистрации, ФИО, дата_рождения,
   образование) - Читал – Книга (название, автор, год_издания) Примечание. В скобках приведены свойства узлов и
   отношения (связи), глагол – это отношение.
2. Разработать и реализовать запрос: какой автор является наиболее читаемым.

### Spark

1. По данным из Elasticsearch сформировать csv-файлы (с внутренней схемой) таблиц «Читатель», «Книга» и сохранить их в
   файловой системе HDFS.
2. Написать запрос select: найти читателей, имеющих задолженности.
3. Реализовать этот запрос в Spark. Построить временную диаграмму его выполнения по результатам работы монитора.

### Pgvector

1. Преобразовать документы 1-го типа в векторы.
2. Сохранить векторы в векторной БД.
3. Найти 3 самых близких документа для какого-либо одного документа.

## Version

python 3.9

## Dependencies

* pydantic = "2.10.6"
* email-validator = "2.2.0"
* pydantic-settings = "2.8.1"
* elasticsearch = "7.17.0"
* elastic-transport = "7.16.0"
* py2neo = "2021.2.4"
* pyspark = "3.5.3"
* ruff = "^0.11.5"
* psycopg2 = "2.9.10"
* Faker = "37.0.0"
* python-dotenv = "1.0.1"
* sentence-transformers = "3.4.1"
