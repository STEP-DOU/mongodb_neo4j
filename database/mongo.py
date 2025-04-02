# database/mongo.py

from pymongo import MongoClient
import pandas as pd
from config.config import MONGO_URI

def connect_mongo(uri=MONGO_URI):
    return MongoClient(uri)

def get_most_common_year(collection):
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    result = list(collection.aggregate(pipeline))
    return result[0] if result else None

def count_movies_after_1999(collection):
    return collection.count_documents({"year": {"$gt": 1999}})

def average_votes_2007(collection):
    pipeline = [
        {"$match": {"year": 2007, "Votes": {"$exists": True}}},
        {"$group": {"_id": None, "avgVotes": {"$avg": "$Votes"}}}
    ]
    result = list(collection.aggregate(pipeline))
    return result[0]["avgVotes"] if result else 0

def get_films_per_year(collection):
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    return list(collection.aggregate(pipeline))

def get_genres(collection):
    all_genres = collection.distinct("genre")
    # extraire les sous-genres s'ils sont séparés par des virgules
    genre_set = set()
    for g in all_genres:
        genre_set.update([x.strip() for x in g.split(",")])
    return sorted(list(genre_set))

def get_top_revenue_film(collection):
    return collection.find_one({"Revenue (Millions)": {"$ne": ""}}, sort=[("Revenue (Millions)", -1)])

def get_directors_with_more_than_5_films(collection):
    pipeline = [
        {"$group": {"_id": "$Director", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 5}}},
        {"$sort": {"count": -1}}
    ]
    return list(collection.aggregate(pipeline))

def get_best_avg_revenue_by_genre(collection):
    pipeline = [
        {"$match": {"Revenue (Millions)": {"$ne": ""}}},
        {"$project": {
            "genre": {"$split": ["$genre", ","]},
            "revenue": "$Revenue (Millions)"
        }},
        {"$unwind": "$genre"},
        {"$group": {"_id": "$genre", "avgRevenue": {"$avg": "$revenue"}}},
        {"$sort": {"avgRevenue": -1}},
        {"$limit": 1}
    ]
    result = list(collection.aggregate(pipeline))
    return result[0] if result else None

def get_top_rated_per_decade(collection):
    pipeline = [
        {"$match": {"rating": {"$exists": True}}},
        {"$project": {
            "title": 1,
            "rating": 1,
            "decade": {"$concat": [
                {"$substr": [{"$subtract": ["$year", {"$mod": ["$year", 10]}]}, 0, 4]},
                "s"
            ]}
        }},
        {"$sort": {"decade": 1, "rating": -1}},
        {"$group": {
            "_id": "$decade",
            "top3": {"$push": {"title": "$title", "rating": "$rating"}}
        }},
        {"$project": {"top3": {"$slice": ["$top3", 3]}}}
    ]
    return list(collection.aggregate(pipeline))

def get_longest_film_per_genre(collection):
    pipeline = [
        {"$project": {
            "title": 1,
            "genre": {"$split": ["$genre", ","]},
            "runtime": "$Runtime (Minutes)"
        }},
        {"$unwind": "$genre"},
        {"$sort": {"runtime": -1}},
        {"$group": {
            "_id": "$genre",
            "title": {"$first": "$title"},
            "runtime": {"$first": "$runtime"}
        }}
    ]
    return list(collection.aggregate(pipeline))

def create_high_score_view(collection):
    pipeline = [
        {"$match": {
            "Metascore": {"$gt": 80},
            "Revenue (Millions)": {"$gt": 50}
        }},
        {"$out": "high_score_films"}
    ]
    collection.aggregate(pipeline)
    return "Vue 'high_score_films' créée avec succès."

def compute_runtime_revenue_correlation(collection):
    df = pd.DataFrame(list(collection.find(
        {"Runtime (Minutes)": {"$exists": True}, "Revenue (Millions)": {"$ne": ""}},
        {"Runtime (Minutes)": 1, "Revenue (Millions)": 1, "_id": 0}
    )))
    if len(df) < 2:
        return None
    df = df.astype({"Runtime (Minutes)": float, "Revenue (Millions)": float})
    return df["Runtime (Minutes)"].corr(df["Revenue (Millions)"])

def get_avg_runtime_by_decade(collection):
    pipeline = [
        {"$project": {
            "decade": {"$subtract": ["$year", {"$mod": ["$year", 10]}]},
            "runtime": "$Runtime (Minutes)"
        }},
        {"$group": {"_id": "$decade", "avgRuntime": {"$avg": "$runtime"}}},
        {"$sort": {"_id": 1}}
    ]
    return list(collection.aggregate(pipeline))

def recommend_film_mongo(collection, preferred_genres, excluded_actor):
    projection = {"title": 1, "genre": 1, "rating": 1, "Votes": 1, "Actors": 1}

    query_steps = [
        {"rating": 7.0, "votes": 10000},
        {"rating": 6.5, "votes": 5000},
        {"rating": 6.0, "votes": 1000},
        {"rating": 0.0, "votes": 0}
    ]

    # Parcours de chaque niveau de filtrage
    for step in query_steps:
        query = {
            "genre": {"$in": preferred_genres},  # Prend tous les genres préférés
            "Actors": {"$not": {"$regex": excluded_actor, "$options": "i"}},
            "rating": {"$gte": step["rating"]},
            "Votes": {"$gte": step["votes"]}
        }
        # Recherche un film correspondant aux critères
        film = collection.find_one(query, projection, sort=[("rating", -1)])
        
        if film:
            # Si un film est trouvé, on l'affiche et retourne
            film["criteria"] = f"Note ≥ {step['rating']}, Votes ≥ {step['votes']}"
            return film

    # Si aucun film n'est trouvé après tous les essais
    return None
