# ================================
# database/mongo.py
# Fonctions pour interagir avec MongoDB
# ================================

# Importation du client MongoDB
from pymongo import MongoClient
# Utilisation de pandas pour les éventuelles manipulations de DataFrame
import pandas as pd
# Importation de l’URI MongoDB depuis le fichier de configuration
from config.config import MONGO_URI

# Connexion à MongoDB à partir de l'URI (par défaut, celui défini dans config)
def connect_mongo(uri=MONGO_URI):
    return MongoClient(uri)

# -------------------------------
# Requêtes d'analyse MongoDB
# -------------------------------

# Retourne l’année avec le plus grand nombre de films
def get_most_common_year(collection):
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},  # Regroupement par année avec comptage
        {"$sort": {"count": -1}},                            # Tri décroissant par nombre de films
        {"$limit": 1}                                        # On garde seulement la première année
    ]
    result = list(collection.aggregate(pipeline))
    return result[0] if result else None  # Retourne le résultat ou None si vide

# Compte le nombre de films sortis après 1999
def count_movies_after_1999(collection):
    return collection.count_documents({"year": {"$gt": 1999}})

# Calcule la moyenne des votes pour les films sortis en 2007
def average_votes_2007(collection):
    pipeline = [
        {"$match": {"year": 2007, "Votes": {"$exists": True}}},       # Filtre les films de 2007 avec des votes
        {"$group": {"_id": None, "avgVotes": {"$avg": "$Votes"}}}     # Calcule la moyenne des votes
    ]
    result = list(collection.aggregate(pipeline))
    return result[0]["avgVotes"] if result else 0

# Donne le nombre de films par année (pour créer un histogramme)
def get_films_per_year(collection):
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},  # Regroupe par année
        {"$sort": {"_id": 1}}                                # Trie chronologiquement
    ]
    return list(collection.aggregate(pipeline))

# Récupère tous les genres distincts dans la base (nettoyés si séparés par des virgules)
def get_genres(collection):
    all_genres = collection.distinct("genre")  # Liste brute des genres
    genre_set = set()
    # Pour chaque champ genre, on découpe par virgule et on nettoie les espaces
    for g in all_genres:
        genre_set.update([x.strip() for x in g.split(",")])
    return sorted(list(genre_set))  # Retourne une liste triée des genres uniques

# Récupère le film ayant généré le plus de revenus
def get_top_revenue_film(collection):
    return collection.find_one({"Revenue (Millions)": {"$ne": ""}}, sort=[("Revenue (Millions)", -1)])

# Récupère les réalisateurs ayant dirigé plus de 5 films
def get_directors_with_more_than_5_films(collection):
    pipeline = [
        {"$group": {"_id": "$Director", "count": {"$sum": 1}}},         # Regroupe les films par réalisateur
        {"$match": {"count": {"$gt": 5}}},                              # Filtre ceux qui en ont plus de 5
        {"$sort": {"count": -1}}                                       # Trie par nombre de films
    ]
    return list(collection.aggregate(pipeline))

# Trouve le genre qui rapporte le plus en moyenne
def get_best_avg_revenue_by_genre(collection):
    pipeline = [
        {"$match": {"Revenue (Millions)": {"$ne": ""}}},               # Garde les films avec revenu renseigné
        {"$project": {
            "genre": {"$split": ["$genre", ","]},                      # Sépare les genres multiples en liste
            "revenue": "$Revenue (Millions)"
        }},
        {"$unwind": "$genre"},                                         # Dénormalise pour un genre par ligne
        {"$group": {"_id": "$genre", "avgRevenue": {"$avg": "$revenue"}}},  # Moyenne des revenus par genre
        {"$sort": {"avgRevenue": -1}},                                 # Trie décroissant
        {"$limit": 1}                                                  # Garde le meilleur genre
    ]
    result = list(collection.aggregate(pipeline))
    return result[0] if result else None



# ==========================
# Fonctions avancées MongoDB
# ==========================

# Récupère les 3 meilleurs films par décennie, selon leur note (rating)
def get_top_rated_per_decade(collection):
    pipeline = [
        {"$match": {"rating": {"$exists": True}}},  # On garde les films qui ont un rating
        {"$project": {
            "title": 1,
            "rating": 1,
            # Calcule la décennie : par ex. 1994 -> "1990s"
            "decade": {"$concat": [
                {"$substr": [{"$subtract": ["$year", {"$mod": ["$year", 10]}]}, 0, 4]},
                "s"
            ]}
        }},
        {"$sort": {"decade": 1, "rating": -1}},  # Trie les films par décennie puis par note décroissante
        {"$group": {
            "_id": "$decade",                     # Groupe les films par décennie
            "top3": {"$push": {"title": "$title", "rating": "$rating"}}  # Stocke tous les films triés
        }},
        {"$project": {"top3": {"$slice": ["$top3", 3]}}}  # Garde les 3 meilleurs par groupe
    ]
    return list(collection.aggregate(pipeline))

# Renvoie le film le plus long par genre
def get_longest_film_per_genre(collection):
    pipeline = [
        {"$project": {
            "title": 1,
            "genre": {"$split": ["$genre", ","]},          # Sépare les genres multiples
            "runtime": "$Runtime (Minutes)"
        }},
        {"$unwind": "$genre"},                             # Dénormalise un genre par ligne
        {"$sort": {"runtime": -1}},                        # Trie par durée décroissante
        {"$group": {
            "_id": "$genre",                               # Groupe par genre
            "title": {"$first": "$title"},                 # Prend le film avec la durée max
            "runtime": {"$first": "$runtime"}
        }}
    ]
    return list(collection.aggregate(pipeline))

# Crée une vue MongoDB contenant les films ayant un score élevé (>80) et revenu > 50M$
def create_high_score_view(collection):
    pipeline = [
        {"$match": {
            "Metascore": {"$gt": 80},
            "Revenue (Millions)": {"$gt": 50}
        }},
        {"$out": "high_score_films"}  # Crée une nouvelle collection appelée "high_score_films"
    ]
    collection.aggregate(pipeline)
    return "Vue 'high_score_films' créée avec succès."

# Calcule la corrélation statistique entre la durée d’un film et son revenu
def compute_runtime_revenue_correlation(collection):
    # On récupère les films ayant une durée et un revenu
    df = pd.DataFrame(list(collection.find(
        {"Runtime (Minutes)": {"$exists": True}, "Revenue (Millions)": {"$ne": ""}},
        {"Runtime (Minutes)": 1, "Revenue (Millions)": 1, "_id": 0}
    )))
    if len(df) < 2:
        return None  # Trop peu de données pour calculer une corrélation
    # Convertit les types vers float pour analyse
    df = df.astype({"Runtime (Minutes)": float, "Revenue (Millions)": float})
    return df["Runtime (Minutes)"].corr(df["Revenue (Millions)"])  # Calcule la corrélation de Pearson

# Calcule la durée moyenne des films par décennie
def get_avg_runtime_by_decade(collection):
    pipeline = [
        {"$project": {
            "decade": {"$subtract": ["$year", {"$mod": ["$year", 10]}]},  # Calcule la décennie (ex: 1994 -> 1990)
            "runtime": "$Runtime (Minutes)"
        }},
        {"$group": {"_id": "$decade", "avgRuntime": {"$avg": "$runtime"}}},  # Moyenne par décennie
        {"$sort": {"_id": 1}}  # Trie chronologiquement
    ]
    return list(collection.aggregate(pipeline))

# Recommande un film à un acteur donné selon ses genres préférés
def recommend_film_mongo(collection, preferred_genres, excluded_actor):
    projection = {"title": 1, "genre": 1, "rating": 1, "Votes": 1, "Actors": 1}

    # Liste d'étapes progressives pour élargir les critères de sélection
    query_steps = [
        {"rating": 7.0, "votes": 10000},  # Niveau exigeant
        {"rating": 6.5, "votes": 5000},   # Moins exigeant
        {"rating": 6.0, "votes": 1000},
        {"rating": 0.0, "votes": 0}       # Tous les films si rien trouvé avant
    ]

    # Teste chaque niveau jusqu’à trouver un film
    for step in query_steps:
        query = {
            "genre": {"$in": preferred_genres},                      # Genres préférés
            "Actors": {"$not": {"$regex": excluded_actor, "$options": "i"}},  # L'acteur ne doit pas être présent
            "rating": {"$gte": step["rating"]},
            "Votes": {"$gte": step["votes"]}
        }
        film = collection.find_one(query, projection, sort=[("rating", -1)])
        
        if film:
            film["criteria"] = f"Note ≥ {step['rating']}, Votes ≥ {step['votes']}"
            return film  # Retourne le premier film trouvé avec les critères

    return None  # Aucun film trouvé avec les genres/critères fournis
