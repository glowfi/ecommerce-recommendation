import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import pickle
from dotenv import find_dotenv, load_dotenv
import os

# Load dotenv
load_dotenv(find_dotenv(".env"))
CB_FILTER_DATA_COLLECTION_NAME = os.getenv("CB_FILTER_COLLECTION_NAME")


class ProductRecommender:
    def __init__(self, db, products_df):

        self.db = db
        self.products_df = pd.DataFrame(products_df)
        self.vectorizer = TfidfVectorizer(max_features=5000, stop_words="english")
        self.tfidf_matrix = self.vectorizer.fit_transform(
            self._create_combined_text(self.products_df)
        )
        self.tfidf_array = self.tfidf_matrix.toarray()
        self.similarity_matrix = cosine_similarity(self.tfidf_array)
        self.similarity_dict = self._create_similarity_dict()

    def _create_combined_text(self, products_df):
        combined_text = []
        for index, row in products_df.iterrows():
            text = (
                row["categoryName"]
                + " "
                + row["title"]
                + " "
                + row["brand"]
                + " "
                + row["description"]
            )
            combined_text.append(text)
        return self._preprocess_text(pd.Series(combined_text))

    def _preprocess_text(self, text_series):
        # Tokenize the text
        tokenized_text = text_series.apply(word_tokenize)

        # Remove stopwords
        stop_words = set(stopwords.words("english"))
        tokenized_text = tokenized_text.apply(
            lambda x: [word for word in x if word.lower() not in stop_words]
        )

        # Lemmatize the tokens
        lemmatizer = WordNetLemmatizer()
        tokenized_text = tokenized_text.apply(
            lambda x: [lemmatizer.lemmatize(word) for word in x]
        )

        # Join the tokens back into a string
        preprocessed_text = tokenized_text.apply(lambda x: " ".join(x))

        return preprocessed_text

    def _create_similarity_dict(self):
        similarity_dict = {}
        for i, product_id in enumerate(self.products_df["_id"]):
            similarity_dict[product_id] = {}
            for j, similar_product_id in enumerate(self.products_df["_id"]):
                if i != j:
                    similarity_dict[product_id][similar_product_id] = (
                        self.similarity_matrix[i, j]
                    )
        return similarity_dict

    async def generate_recommendations(self, product_id, num_recommendations=5):

        similar_products = sorted(
            self.similarity_dict[product_id].items(), key=lambda x: x[1], reverse=True
        )
        curr_recommendations = [
            product_id for product_id, _ in similar_products[:num_recommendations]
        ]

        res = []
        for id in curr_recommendations:
            currProd = await self.db[CB_FILTER_DATA_COLLECTION_NAME].find_one(
                {"_id": id}
            )
            res.append(currProd)

        return res

    def save_similarity_matrix(self, filename):
        with open(filename, "wb") as f:
            pickle.dump(self.similarity_matrix, f)

    def load_similarity_matrix(self, filename):
        with open(filename, "rb") as f:
            self.similarity_matrix = pickle.load(f)
