# Importation de la classe GraphDatabase depuis le module officiel Neo4j pour Python.
# Cette classe permet d'établir une connexion avec une base de données Neo4j
# et d'exécuter des requêtes Cypher via un driver.
from neo4j import GraphDatabase

# Importation des paramètres de connexion à Neo4j depuis le fichier de configuration.
# Cela inclut :
# - NEO4J_URI : l'adresse du serveur Neo4j (ex. "bolt://localhost:7687")
# - NEO4J_USER : le nom d'utilisateur pour se connecter à Neo4j
# - NEO4J_PASSWORD : le mot de passe associé à cet utilisateur
from config.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD


# ==========================
# Connexion à Neo4j
# ==========================

# Établit la connexion au serveur Neo4j à partir des identifiants définis dans le fichier de config
def connect_neo4j(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD):
    return GraphDatabase.driver(uri, auth=(user, password))

# Teste la connexion à Neo4j en renvoyant un message de confirmation
def test_connection(driver):
    with driver.session() as session:
        result = session.run("RETURN 'Connexion à Neo4j réussie !' AS message")
        return result.single()["message"]

# ==========================
# Fonctions de requêtage Neo4j
# ==========================

# Renvoie la liste des 50 premiers titres de films, triés par ordre alphabétique
def get_all_films(driver):
    with driver.session() as session:
        result = session.run("MATCH (f:Film) RETURN f.title AS title ORDER BY f.title LIMIT 50")
        return [record["title"] for record in result]

# Renvoie tous les noms de réalisateurs, triés par ordre alphabétique
def get_all_directors(driver):
    with driver.session() as session:
        result = session.run("MATCH (d:Director) RETURN d.name AS name ORDER BY d.name")
        return [record["name"] for record in result]

# Récupère tous les films réalisés par un réalisateur donné
def get_films_by_director(driver, director_name):
    with driver.session() as session:
        result = session.run(
            "MATCH (d:Director {name: $name})-[:REALISE]->(f:Film) RETURN f.title AS title ORDER BY f.year",
            name=director_name
        )
        return [record["title"] for record in result]

# Trouve l’acteur ayant joué dans le plus de films
def get_most_active_actor(driver):
    with driver.session() as session:
        query = """
        MATCH (a:Actor)-[:A_JOUE]->(f:Film)
        RETURN a.name AS actor, COUNT(f) AS nb_films
        ORDER BY nb_films DESC
        LIMIT 1
        """
        result = session.run(query)
        return result.single()

# Liste les co-acteurs ayant joué avec un acteur donné (par défaut Anne Hathaway)
def get_actors_who_played_with(driver, actor_name="Anne Hathaway"):
    with driver.session() as session:
        query = """
        MATCH (a1:Actor {name: $actor_name})-[:A_JOUE]->(f:Film)<-[:A_JOUE]-(a2:Actor)
        WHERE a1 <> a2
        RETURN DISTINCT a2.name AS co_actor
        ORDER BY co_actor
        """
        result = session.run(query, {"actor_name": actor_name})
        return [record["co_actor"] for record in result]

# Renvoie l’acteur ayant généré le plus de revenus cumulés
def get_top_grossing_actor(driver):
    with driver.session() as session:
        query = """
        MATCH (a:Actor)-[:A_JOUE]->(f:Film)
        WHERE f.revenue IS NOT NULL
        RETURN a.name AS actor, SUM(toFloat(f.revenue)) AS total_revenue
        ORDER BY total_revenue DESC
        LIMIT 1
        """
        result = session.run(query)
        return result.single()

# Calcule la moyenne du nombre de votes sur l’ensemble des films
def get_average_votes(driver):
    with driver.session() as session:
        query = """
        MATCH (f:Film)
        WHERE f.votes IS NOT NULL
        RETURN avg(toFloat(f.votes)) AS avg_votes
        """
        result = session.run(query)
        return result.single()

# Trouve le genre de film le plus courant dans la base
def get_most_common_genre(driver):
    with driver.session() as session:
        query = """
        MATCH (f:Film)-[:APPARTIENT_A]->(g:Genre)
        RETURN g.name AS genre, COUNT(f) AS nb_films
        ORDER BY nb_films DESC
        LIMIT 1
        """
        result = session.run(query)
        return result.single()

# Récupère les films dans lesquels ont joué les co-acteurs d’un acteur donné
def get_films_played_by_coactors(driver, actor_name):
    with driver.session() as session:
        query = """
        MATCH (me:Actor {name: $name})-[:A_JOUE]->(f1:Film)<-[:A_JOUE]-(co:Actor)
        WHERE me <> co
        MATCH (co)-[:A_JOUE]->(f2:Film)
        RETURN DISTINCT f2.title AS film
        ORDER BY film
        """
        result = session.run(query, {"name": actor_name})
        return [record["film"] for record in result]

# Récupère tous les noms d’acteurs dans la base
def get_all_actors(driver):
    with driver.session() as session:
        result = session.run("MATCH (a:Actor) RETURN a.name AS name ORDER BY name")
        return [record["name"] for record in result]



# ==========================
# Fonctions avancées Neo4j
# ==========================

# Récupère le réalisateur ayant collaboré avec le plus grand nombre d’acteurs distincts
def get_director_with_most_actors(driver):
    with driver.session() as session:
        query = """
        MATCH (d:Director)-[:REALISE]->(f:Film)<-[:A_JOUE]-(a:Actor)
        RETURN d.name AS director, COUNT(DISTINCT a) AS nb_actors
        ORDER BY nb_actors DESC
        LIMIT 1
        """
        result = session.run(query)
        return result.single()

# Récupère les films qui ont le plus d’acteurs (par défaut top 5)
def get_most_connected_films(driver, limit=5):
    with driver.session() as session:
        query = """
        MATCH (a:Actor)-[:A_JOUE]->(f:Film)
        RETURN f.title AS title, COUNT(a) AS nb_acteurs
        ORDER BY nb_acteurs DESC
        LIMIT $limit
        """
        result = session.run(query, {"limit": limit})
        return [{"title": r["title"], "actors": r["nb_acteurs"]} for r in result]

# Trouve les acteurs ayant travaillé avec le plus de réalisateurs différents
def get_actors_with_most_directors(driver, limit=5):
    with driver.session() as session:
        query = """
        MATCH (a:Actor)-[:A_JOUE]->(f:Film)<-[:REALISE]-(d:Director)
        RETURN a.name AS actor, COUNT(DISTINCT d) AS nb_directors
        ORDER BY nb_directors DESC
        LIMIT $limit
        """
        result = session.run(query, {"limit": limit})
        return [{"actor": r["actor"], "directors": r["nb_directors"]} for r in result]

# Recommande un film à un acteur selon son genre préféré
def recommend_film_by_genre(driver, actor_name):
    with driver.session() as session:
        query = """
        MATCH (a:Actor {name: $name})-[:A_JOUE]->(:Film)-[:APPARTIENT_A]->(g:Genre)
        WITH a, g, COUNT(*) AS freq
        ORDER BY freq DESC
        LIMIT 1
        MATCH (rec:Film)-[:APPARTIENT_A]->(g)
        WHERE NOT (a)-[:A_JOUE]->(rec)
        RETURN rec.title AS title, g.name AS genre
        LIMIT 1
        """
        result = session.run(query, {"name": actor_name})
        return result.single()

# Crée les relations d'influence entre réalisateurs ayant réalisé des films de même genre
def create_influence_relationships(driver):
    with driver.session() as session:
        query = """
        MATCH (d1:Director)-[:REALISE]->(:Film)-[:APPARTIENT_A]->(g:Genre)<-[:APPARTIENT_A]-(:Film)<-[:REALISE]-(d2:Director)
        WHERE d1 <> d2
        MERGE (d1)-[:INFLUENCE_PAR]->(d2)
        """
        session.run(query)
    return "Relations :INFLUENCE_PAR créées entre réalisateurs avec genres communs."

# Calcule le chemin le plus court entre deux acteurs (via la relation A_JOUE)
def get_shortest_path_between_actors(driver, actor1, actor2):
    with driver.session() as session:
        query = """
        MATCH path = shortestPath(
            (a1:Actor {name: $actor1})-[:A_JOUE*]-(a2:Actor {name: $actor2})
        )
        RETURN path
        """
        result = session.run(query, {"actor1": actor1, "actor2": actor2})
        record = result.single()
        if not record:
            return None

        path = record["path"]
        # On retourne les noms des acteurs (ou titres de films) sur le chemin trouvé
        nodes = [node["name"] if "name" in node else node["title"] for node in path.nodes]
        return nodes

# Crée les relations A_JOUE_AVEC entre tous les acteurs ayant joué dans le même film
def create_actor_collaboration_edges(driver):
    with driver.session() as session:
        query = """
        MATCH (a1:Actor)-[:A_JOUE]->(f:Film)<-[:A_JOUE]-(a2:Actor)
        WHERE a1 <> a2
        MERGE (a1)-[:A_JOUE_AVEC]-(a2)
        """
        session.run(query)
    return "Relations :A_JOUE_AVEC créées entre acteurs ayant partagé un film."

# Utilise l'algorithme Louvain de Neo4j GDS pour détecter des communautés d’acteurs
def detect_actor_communities(driver):
    with driver.session() as session:
        # Supprimer le graphe en mémoire s’il existe
        session.run("CALL gds.graph.drop('actorGraph', false)")
        # Créer un graphe projeté basé sur les relations A_JOUE_AVEC
        session.run("""
        CALL gds.graph.project(
            'actorGraph',
            'Actor',
            {
                A_JOUE_AVEC: {
                    type: 'A_JOUE_AVEC',
                    orientation: 'UNDIRECTED'
                }
            }
        )
        """)
        # Exécution de l'algorithme Louvain pour détecter les communautés
        result = session.run("""
        CALL gds.louvain.stream('actorGraph')
        YIELD nodeId, communityId
        RETURN gds.util.asNode(nodeId).name AS actor, communityId
        ORDER BY communityId, actor
        """)
        return result.data()


# ==========================
# Fonctions pour l’analyse croisée MongoDB / Neo4j
# ==========================

# Trouve des paires de films appartenant à un même genre mais réalisés par des personnes différentes
def get_films_with_common_genres_diff_directors(driver, limit=10):
    with driver.session() as session:
        query = """
        MATCH (f1:Film)-[:APPARTIENT_A]->(g:Genre)<-[:APPARTIENT_A]-(f2:Film),
              (f1)<-[:REALISE]-(d1:Director),
              (f2)<-[:REALISE]-(d2:Director)
        WHERE f1 <> f2 AND d1 <> d2
        RETURN DISTINCT f1.title AS film1, d1.name AS director1,
                        f2.title AS film2, d2.name AS director2,
                        g.name AS genre
        LIMIT $limit
        """
        result = session.run(query, {"limit": limit})
        return result.data()

# Identifie les genres préférés d’un acteur donné, en fonction du nombre de films associés
def get_preferred_genres_for_actor(driver, actor_name, limit=3):
    with driver.session() as session:
        query = """
        MATCH (a:Actor {name: $name})-[:A_JOUE]->(:Film)-[:APPARTIENT_A]->(g:Genre)
        RETURN g.name AS genre, COUNT(*) AS freq
        ORDER BY freq DESC
        LIMIT $limit
        """
        result = session.run(query, {"name": actor_name, "limit": limit})
        return [record["genre"] for record in result]

# Crée une relation :CONCURRENCE entre deux réalisateurs ayant fait des films similaires la même année
def create_director_concurrence_relationships(driver):
    with driver.session() as session:
        query = """
        MATCH (d1:Director)-[:REALISE]->(f1:Film)-[:APPARTIENT_A]->(g:Genre)<-[:APPARTIENT_A]-(f2:Film)<-[:REALISE]-(d2:Director)
        WHERE d1 <> d2 AND f1.year = f2.year
        MERGE (d1)-[:CONCURRENCE]->(d2)
        """
        session.run(query)
    return "Relations :CONCURRENCE créées entre réalisateurs avec films similaires la même année."

# Renvoie les collaborations fréquentes entre acteurs et réalisateurs, avec leurs performances (revenu et votes)
def get_frequent_collaborations_with_success(driver, min_collaborations=1):
    with driver.session() as session:
        query = """
        MATCH (a:Actor)-[:A_JOUE]->(f:Film)<-[:REALISE]-(d:Director)
        WITH a, d, COUNT(f) AS collaborations, 
             AVG(toFloat(f.revenue)) AS avg_revenue, 
             AVG(toFloat(f.votes)) AS avg_votes
        WHERE collaborations >= $min_collaborations
        RETURN a.name AS actor, d.name AS director, collaborations, avg_revenue, avg_votes
        ORDER BY collaborations DESC
        """
        result = session.run(query, {"min_collaborations": min_collaborations})
        return result.data()
