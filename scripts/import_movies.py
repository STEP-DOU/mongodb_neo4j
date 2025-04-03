import json
from pymongo import MongoClient
from config.config import MONGO_URI

# Connexion à MongoDB
client = MongoClient(MONGO_URI)
db = client["entertainment"]
collection = db["films"]

# Lecture des films à partir du fichier JSON
with open("data/movies.json", "r", encoding="utf-8") as f:
    movies = [json.loads(line) for line in f if line.strip()]

collection.insert_many(movies)
print("Importation terminée avec succès.")
