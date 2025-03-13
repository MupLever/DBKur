from sentence_transformers import SentenceTransformer


class InsertValuesScript:
    def __init__(self, client, data):
        self.client = client
        self.data = data

        # Загрузка модели для преобразования текста в векторы
        self.model = SentenceTransformer("all-MiniLM-L6-v2")

    def transform_doc_to_vector(self):
        """Преобразование документов в векторы"""
        texts = ["".format() for doc in self.data]
        vectors = self.model.encode(texts).tolist()
        return vectors

    def run(self):
        vectors = self.transform_doc_to_vector()
        with self.client.cursor() as cur:
            # Сохранение векторов в базу данных
            for doc, vector in zip(self.data, vectors):
                cur.execute(
                    """INSERT INTO readers (registration_date, fullname, birthdate, education, vector)
                    VALUES (%s, %s, %s, %s, %s)""",
                    (
                        doc["дата_регистрации"],
                        doc["ФИО"],
                        doc["дата_рождения"],
                        doc["образование"],
                        vector,
                    ),
                )
        # Сохранение изменений
        self.client.commit()
