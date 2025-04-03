# scripts/import_to_neo4j.py

from pymongo import MongoClient
from neo4j import GraphDatabase
from config.config import MONGO_URI, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

# Connexions
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["entertainment"]
collection = db["films"]

neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# Importation des données de MongoDB vers Neo4j
def import_data():
    with neo4j_driver.session() as session:
        for film in collection.find():
            title = film.get("title")
            year = film.get("year")
            rating = film.get("rating")
            votes = film.get("Votes")
            revenue = film.get("Revenue (Millions)")
            director = film.get("Director")
            actors_str = film.get("Actors")

            if not title:
                continue

            # Création du nœud Film
            query_film = """
            MERGE (f:Film {title: $title})
            SET f.year = $year,
                f.rating = $rating,
                f.votes = $votes,
                f.revenue = $revenue
            """
            session.run(query_film, {
                "title": title,
                "year": year,
                "rating": rating,
                "votes": votes,
                "revenue": revenue
            })

            # Création du réalisateur et relation REALISE
            if director:
                session.run("""
                    MERGE (d:Director {name: $director})
                    MERGE (f:Film {title: $title})
                    MERGE (d)-[:REALISE]->(f)
                """, {"director": director.strip(), "title": title})

            # Création des acteurs et relation A_JOUE
            if actors_str:
                actors = [a.strip() for a in actors_str.split(",")]
                for actor in actors:
                    session.run("""
                        MERGE (a:Actor {name: $actor})
                        MERGE (f:Film {title: $title})
                        MERGE (a)-[:A_JOUE]->(f)
                    """, {"actor": actor, "title": title})

            # Création des genres et relations
            genre_str = film.get("genre")
            if genre_str:
                genres = [g.strip() for g in genre_str.split(",")]
                for genre in genres:
                    session.run("""
                        MERGE (g:Genre {name: $genre})
                        MERGE (f:Film {title: $title})
                        MERGE (f)-[:APPARTIENT_A]->(g)
                    """, {"genre": genre, "title": title})

    print("✅ Importation des acteurs et relations réussie.")

if __name__ == "__main__":
    import_data()
