import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from pymongo import MongoClient
from neo4j import GraphDatabase
from config.config import MONGO_URI, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

# Connexion à MongoDB et Neo4j
client = MongoClient(MONGO_URI)
db = client["entertainment"]
collection = db["films"]

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# Importation des acteurs et création des relations
def import_actors():
    with driver.session() as session:
        for film in collection.find():
            title = film.get("title")
            actors = film.get("actors", [])
            if title and isinstance(actors, list):
                session.run("MERGE (f:Film {title: $title})", title=title)
                for actor in actors:
                    if isinstance(actor, str) and actor.strip():
                        session.run("MERGE (a:Actor {name: $name})", name=actor.strip())
                        session.run("""
                            MATCH (a:Actor {name: $name}), (f:Film {title: $title})
                            MERGE (a)-[:A_JOUE]->(f)
                        """, name=actor.strip(), title=title)

if __name__ == "__main__":
    import_actors()
    print("✔ Import terminé : acteurs + relations A_JOUE")
