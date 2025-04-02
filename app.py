# app.py

# Importation de Streamlit, le framework utilisé pour créer l'application web interactive
import streamlit as st

# Importation des paramètres de configuration (URI des bases de données, utilisateur et mot de passe pour Neo4j)
from config.config import MONGO_URI, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

# --- IMPORTS POUR MONGODB ---

# Import des fonctions définies dans le module mongo.py pour interagir avec la base de données MongoDB
from database.mongo import (
    connect_mongo,                                # Fonction pour établir la connexion à MongoDB
    get_most_common_year,                         # Renvoie l'année ayant le plus de films dans la base
    count_movies_after_1999,                      # Compte les films sortis après 1999
    average_votes_2007,                           # Calcule la moyenne des votes des films sortis en 2007
    get_films_per_year,                           # Donne un histogramme du nombre de films par année
    get_genres,                                   # Liste les genres de films présents dans la collection
    get_top_revenue_film,                         # Récupère le film ayant généré le plus de revenus
    get_directors_with_more_than_5_films,         # Renvoie les réalisateurs ayant réalisé plus de 5 films
    get_best_avg_revenue_by_genre,                # Calcule le revenu moyen par genre et retourne celui qui rapporte le plus
    get_top_rated_per_decade,                     # Récupère le top 3 des films les mieux notés pour chaque décennie
    get_longest_film_per_genre,                   # Récupère le film le plus long pour chaque genre
    create_high_score_view,                       # Crée une vue MongoDB filtrée (films avec metascore > 80 et revenus > 50M)
    compute_runtime_revenue_correlation,          # Calcule la corrélation entre la durée et le revenu
    get_avg_runtime_by_decade,                    # Calcule la durée moyenne des films par décennie
    recommend_film_mongo                          # Recommande un film basé sur les genres favoris d’un acteur
)

# --- IMPORTS POUR NEO4J ---

# Import des fonctions définies dans le module neo4j.py pour interagir avec la base de données graphique Neo4j
from database.neo4j import (
    connect_neo4j,                                # Fonction pour établir la connexion à Neo4j
    test_connection,                              # Fonction pour tester la connexion avec la base Neo4j
    get_all_films,                                # Liste tous les films dans la base Neo4j
    get_all_directors,                            # Liste tous les réalisateurs
    get_films_by_director,                        # Récupère les films réalisés par un réalisateur donné
    get_most_active_actor,                        # Renvoie l’acteur ayant joué dans le plus de films
    get_actors_who_played_with,                   # Liste les acteurs ayant joué avec un acteur donné
    get_top_grossing_actor,                       # Renvoie l’acteur ayant généré le plus de revenus
    get_average_votes,                            # Calcule la moyenne des votes de tous les films
    get_most_common_genre,                        # Renvoie le genre le plus représenté dans la base
    get_films_played_by_coactors,                 # Donne les films dans lesquels les co-acteurs d’un acteur ont joué
    get_all_actors,                               # Liste tous les acteurs dans la base
    get_director_with_most_actors,                # Renvoie le réalisateur ayant collaboré avec le plus d’acteurs différents
    get_most_connected_films,                     # Renvoie les films avec le plus d’acteurs (fortement connectés)
    get_actors_with_most_directors,               # Renvoie les acteurs ayant travaillé avec le plus de réalisateurs différents
    recommend_film_by_genre,                      # Recommande un film à un acteur selon ses genres préférés
    create_influence_relationships,               # Crée des relations d’influence entre réalisateurs (si genres similaires)
    get_shortest_path_between_actors,             # Trouve le chemin le plus court entre deux acteurs dans le graphe
    create_actor_collaboration_edges,             # Crée des relations d’acteurs ayant collaboré dans un film
    detect_actor_communities,                     # Détecte des communautés d’acteurs (clustering, algorithme Louvain)
    get_films_with_common_genres_diff_directors,  # Renvoie les films de même genre mais réalisés par des personnes différentes
    get_preferred_genres_for_actor,               # Identifie les genres préférés d’un acteur selon ses films
    create_director_concurrence_relationships,    # Crée des relations de concurrence entre réalisateurs ayant produit des films similaires en même temps
    get_frequent_collaborations_with_success      # Renvoie les collaborations fréquentes (acteur-réalisateur) avec des bons résultats (revenus, votes)
)


# Configuration de la page Streamlit : définit le titre de l'onglet du navigateur et le mode d'affichage en pleine largeur
st.set_page_config(page_title="NoSQL Explorer", layout="wide")

# Affiche le titre principal en haut de l'application web
st.title("Projet NoSQL – MongoDB & Neo4j Explorer")

# Crée un menu latéral (sidebar) avec des boutons radio pour sélectionner la base de données à explorer
# Trois options sont proposées : MongoDB, Neo4j et une analyse croisée entre les deux
section = st.sidebar.radio("📂 Choisir une base", ["MongoDB", "Neo4j", "Analyse croisée"])


# --- MongoDB Section ---
if section == "MongoDB":
    st.header("📦 Exploration de la base MongoDB")
    
    mongo_client = connect_mongo(MONGO_URI)
    db = mongo_client["entertainment"]
    collection = db["films"]
    
    st.subheader("🎯 Requêtes MongoDB")

    if st.button("📅 Année avec le plus de films"):
        result = get_most_common_year(collection)
        st.success(f"Année : {result['_id']} avec {result['count']} films.")

    if st.button("🎬 Nombre de films après 1999"):
        count = count_movies_after_1999(collection)
        st.info(f"Nombre de films sortis après 1999 : {count}")

    if st.button("⭐ Moyenne des votes en 2007"):
        avg = average_votes_2007(collection)
        st.info(f"Moyenne des votes (2007) : {avg:.2f}")

    if st.button("📈 Histogramme des films par année"):
        data = get_films_per_year(collection)
        st.bar_chart({d['_id']: d['count'] for d in data})

    if st.button("🎭 Genres de films disponibles"):
        genres = get_genres(collection)
        st.write(genres)

    if st.button("💰 Film ayant généré le plus de revenus"):
        film = get_top_revenue_film(collection)
        if film:
            st.write(film)
        else:
            st.warning("Aucun film avec revenu renseigné.")

    if st.button("🎬 Réalisateurs avec plus de 5 films"):
        directors = get_directors_with_more_than_5_films(collection)
        st.write(directors)

    if st.button("🏆 Genre rapportant le plus en moyenne"):
        genre = get_best_avg_revenue_by_genre(collection)
        if genre:
            st.success(f"Genre : {genre['_id'].strip()} – Revenu moyen : {genre['avgRevenue']:.2f} M$")
        else:
            st.warning("Aucun genre trouvé avec revenus valides.")

    if st.button("🎖️ Top 3 films par décennie (rating)"):
        data = get_top_rated_per_decade(collection)
        for d in data:
            st.markdown(f"**{d['_id']}** :")
            for film in d['top3']:
                title = film.get('title', 'Titre inconnu')
                rating = film.get('rating', 'Non classé')
                st.markdown(f"- {title} ({rating})")

    if st.button("⏱️ Film le plus long par genre"):
        data = get_longest_film_per_genre(collection)
        for d in data:
            st.markdown(f"**{d['_id'].strip()}** : {d['title']} ({d['runtime']} min)")

    if st.button("🔍 Créer la vue MongoDB (score > 80, revenu > 50M)"):
        msg = create_high_score_view(collection)
        st.success(msg)

    if st.button("📊 Corrélation durée / revenu"):
        corr = compute_runtime_revenue_correlation(collection)
        if corr is not None:
            st.info(f"Corrélation (runtime vs revenue) : {corr:.3f}")
        else:
            st.warning("Pas assez de données pour calculer la corrélation.")

    if st.button("📉 Durée moyenne des films par décennie"):
        data = get_avg_runtime_by_decade(collection)
        decades = [d['_id'] for d in data]
        avg_runtime = [d['avgRuntime'] for d in data]
        st.line_chart(dict(zip(decades, avg_runtime)))



# --- Neo4j Section ---
elif section == "Neo4j":
    # Titre principal pour cette section dédiée à Neo4j
    st.header("🔗 Exploration de la base Neo4j")

    # Connexion au serveur Neo4j via la fonction connect_neo4j
    driver = connect_neo4j()

    # Bouton pour tester si la connexion à Neo4j fonctionne bien
    if st.button("✅ Tester la connexion à Neo4j"):
        try:
            message = test_connection(driver)
            st.success(message)  # Affiche un message de succès si la connexion est OK
        except Exception as e:
            st.error(f"Erreur de connexion : {e}")  # Affiche l’erreur si échec

    # Affiche la liste des films présents dans la base Neo4j
    st.subheader("🎬 Lister les films présents dans Neo4j")
    films = get_all_films(driver)
    st.write(films)

    # Permet de sélectionner un réalisateur et d'afficher ses films
    st.subheader("🎥 Lister les réalisateurs")
    directors = get_all_directors(driver)
    selected_director = st.selectbox("Choisir un réalisateur", directors)

    if selected_director:
        films_by_director = get_films_by_director(driver, selected_director)
        st.write(f"Films réalisés par **{selected_director}** :")
        st.write(films_by_director)

    # Affiche l’acteur ayant joué dans le plus de films
    st.subheader("🎭 Acteur ayant joué dans le plus de films")
    if st.button("Afficher l'acteur le plus actif"):
        actor_info = get_most_active_actor(driver)
        if actor_info:
            st.success(f"{actor_info['actor']} a joué dans {actor_info['nb_films']} films.")
        else:
            st.warning("Aucun acteur trouvé.")

    # Affiche les acteurs ayant partagé un film avec Anne Hathaway
    st.subheader("🤝 Acteurs ayant joué avec Anne Hathaway")
    if st.button("Afficher les acteurs ayant partagé un film avec Anne Hathaway"):
        co_actors = get_actors_who_played_with(driver, "Anne Hathaway")
        if co_actors:
            st.write(f"{len(co_actors)} acteur(s) trouvé(s) :")
            st.write(co_actors)
        else:
            st.warning("Aucun acteur trouvé ou Anne Hathaway absente du graphe.")

    # Affiche l’acteur ayant généré le plus de revenus
    st.subheader("💰 Acteur ayant généré le plus de revenus")
    if st.button("Afficher l'acteur le plus rentable"):
        actor = get_top_grossing_actor(driver)
        if actor:
            st.success(f"{actor['actor']} – {actor['total_revenue']:.2f} M$")
        else:
            st.warning("Aucun acteur avec revenus disponibles.")

    # Affiche la moyenne des votes des films
    st.subheader("⭐ Moyenne des votes des films")
    if st.button("Afficher la moyenne des votes"):
        avg = get_average_votes(driver)
        if avg:
            st.success(f"Moyenne des votes : {avg['avg_votes']:.2f}")
        else:
            st.warning("Aucune donnée de votes trouvée.")

    # Genre le plus fréquent dans la base
    st.subheader("🎬 Genre le plus représenté")
    if st.button("Afficher le genre le plus fréquent"):
        genre = get_most_common_genre(driver)
        if genre:
            st.success(f"Genre : {genre['genre']} – Nombre de films : {genre['nb_films']}")
        else:
            st.warning("Aucun genre trouvé dans la base.")

    # Films dans lesquels les co-acteurs du comédien sélectionné ont joué
    st.subheader("🎞️ Films dans lesquels les co-acteurs ont joué")
    actors = get_all_actors(driver)
    selected_actor = st.selectbox("Choisir un acteur", actors)
    if st.button("Afficher les films joués par ses co-acteurs"):
        films = get_films_played_by_coactors(driver, selected_actor)
        if films:
            st.info(f"{len(films)} film(s) trouvés :")
            st.write(films)
        else:
            st.warning("Aucun film trouvé ou acteur inconnu.")

    # Réalisateur ayant travaillé avec le plus d’acteurs différents
    st.subheader("🎬 Réalisateur ayant travaillé avec le plus d'acteurs distincts")
    if st.button("Afficher le réalisateur le plus collaboratif"):
        director = get_director_with_most_actors(driver)
        if director:
            st.success(f"{director['director']} – {director['nb_actors']} acteur(s) différents")
        else:
            st.warning("Aucun réalisateur ou acteur trouvé dans la base.")

    # Films avec le plus d’acteurs
    st.subheader("🎞️ Films avec le plus d'acteurs")
    if st.button("Afficher les films les plus connectés"):
        top_films = get_most_connected_films(driver)
        if top_films:
            for film in top_films:
                st.markdown(f"- **{film['title']}** : {film['actors']} acteurs")
        else:
            st.warning("Aucun film trouvé.")

    # Acteurs ayant travaillé avec le plus de réalisateurs
    st.subheader("🎭 Top 5 des acteurs ayant travaillé avec le plus de réalisateurs différents")
    if st.button("Afficher les 5 acteurs les plus connectés aux réalisateurs"):
        top_actors = get_actors_with_most_directors(driver)
        if top_actors:
            for a in top_actors:
                st.markdown(f"- **{a['actor']}** : {a['directors']} réalisateurs")
        else:
            st.warning("Aucun résultat.")

    # Recommandation personnalisée d’un film pour un acteur selon ses genres préférés
    st.subheader("🎯 Recommander un film à un acteur selon ses genres préférés")
    actor_for_reco = st.selectbox("Choisir un acteur pour la recommandation", actors)

    if st.button("Recommander un film"):
        reco = recommend_film_by_genre(driver, actor_for_reco)
        if reco:
            st.success(f"Film recommandé pour **{actor_for_reco}** : *{reco['title']}* (Genre : {reco['genre']})")
        else:
            st.warning("Aucune recommandation trouvée (acteur trop spécialisé ou tous les films déjà vus).")

    # Création des relations d’influence entre réalisateurs (selon genres similaires)
    st.subheader("🔁 Relations d'influence entre réalisateurs")
    if st.button("Créer les relations :INFLUENCE_PAR"):
        msg = create_influence_relationships(driver)
        st.success(msg)

    # Trouver le plus court chemin entre deux acteurs
    st.subheader("🧭 Chemin le plus court entre deux acteurs")
    actor_a = st.selectbox("Acteur de départ", actors, key="actor_a")
    actor_b = st.selectbox("Acteur d'arrivée", actors, key="actor_b")

    if st.button("Trouver le plus court chemin entre ces deux acteurs"):
        if actor_a == actor_b:
            st.warning("Sélectionne deux acteurs différents.")
        else:
            path = get_shortest_path_between_actors(driver, actor_a, actor_b)
            if path:
                st.info(f"Chemin le plus court entre **{actor_a}** et **{actor_b}** :")
                st.write(" ➡️ ".join(path))
            else:
                st.error("Aucun chemin trouvé entre ces deux acteurs.")

    # Détection des communautés d’acteurs grâce à l’algorithme Louvain
    st.subheader("🧠 Détection des communautés d'acteurs (Louvain)")

    # Création des relations de collaboration (A_JOUE_AVEC)
    if st.button("Créer les relations A_JOUE_AVEC"):
        msg = create_actor_collaboration_edges(driver)
        st.success(msg)

    # Lancer l’algorithme Louvain pour détecter les communautés
    if st.button("Lancer la détection des communautés avec Louvain"):
        result = detect_actor_communities(driver)
        if result:
            current_community = None
            for r in result:
                if r['communityId'] != current_community:
                    current_community = r['communityId']
                    st.markdown(f"### 🎯 Communauté {current_community}")
                st.markdown(f"- {r['actor']}")
        else:
            st.warning("Aucune communauté détectée ou erreur GDS.")






# --- Analyse croisée ---
elif section == "Analyse croisée":
    # Titre principal de la section d’analyse croisée entre MongoDB et Neo4j
    st.header("🔄 Analyse croisée MongoDB & Neo4j")

    # Connexion à la base Neo4j (pour exploiter les données graphiques)
    driver = connect_neo4j()

    # Importation de la fonction spécifique pour récupérer des films ayant des genres communs
    # mais réalisés par des personnes différentes (analyse de similarité croisée)
    from database.neo4j import get_films_with_common_genres_diff_directors

    # Sous-section : recherche de films similaires (même genre) mais avec des réalisateurs différents
    st.subheader("🎬 Films avec genres en commun mais réalisateurs différents (27)")

    # Bouton pour lancer cette analyse
    if st.button("Afficher les correspondances"):
        results = get_films_with_common_genres_diff_directors(driver)
        if results:
            # Affichage de chaque correspondance sous forme lisible
            for r in results:
                st.markdown(
                    f"- **{r['film1']}** (*{r['director1']}*) & **{r['film2']}** (*{r['director2']}*) – Genre commun : _{r['genre']}_"
                )
        else:
            # Message si aucun résultat n’est trouvé
            st.warning("Aucune correspondance trouvée.")

    # Importation des fonctions nécessaires à la recommandation croisée
    # - depuis MongoDB : fonction de recommandation de films
    # - depuis Neo4j : récupération des genres préférés d’un acteur
    from database.mongo import recommend_film_mongo
    from database.neo4j import get_preferred_genres_for_actor

    # Connexion à MongoDB pour pouvoir faire la recommandation finale
    mongo_client = connect_mongo(MONGO_URI)
    collection = mongo_client["entertainment"]["films"]

    # Sous-section : Recommandation croisée (Neo4j pour les préférences, MongoDB pour les films)
    st.subheader("🍿 Recommandation intelligente croisée (Neo4j + MongoDB) (28)")



    # Sélection d’un acteur pour générer une recommandation personnalisée
    selected_actor = st.selectbox("Choisir un acteur", get_all_actors(driver))

    # Lorsqu’on clique sur le bouton, on lance une recommandation croisée
    if st.button("Recommander un film à cet acteur"):
        # Étape 1 : On récupère les genres préférés de l’acteur depuis Neo4j (analyse de ses rôles précédents)
        genres = get_preferred_genres_for_actor(driver, selected_actor)
        if genres:
            # Affichage des genres préférés détectés
            st.markdown(f"Génération d'une recommandation basée sur les genres préférés : {', '.join(genres)}")
            # Étape 2 : On cherche dans MongoDB un film de ces genres que l’acteur n’a pas encore vu
            film = recommend_film_mongo(collection, genres, selected_actor)
            if film:
                # Si un film est trouvé, on l’affiche avec ses caractéristiques
                st.success(f"🎬 Titre : **{film['title']}**")
                st.markdown(f"- 🎭 Genres : {film['genre']}")
                st.markdown(f"- ⭐ Note : {film['rating']}")
                st.markdown(f"- 👥 Votes : {film['Votes']}")
                st.markdown(f"Critères utilisés : {film['criteria']}")
            else:
                st.warning("Aucune recommandation trouvée avec ces critères.")
        else:
            st.warning("Genres préférés introuvables pour cet acteur.")

    # --- Relations de concurrence entre réalisateurs ---
    # On importe la fonction nécessaire depuis Neo4j (juste avant l'utilisation)
    from database.neo4j import create_director_concurrence_relationships

    st.subheader("⚔️ Relations de concurrence entre réalisateurs (29)")

    # Si on clique sur le bouton, on crée les relations :CONCURRENCE entre réalisateurs
    # Cela permet d'identifier des réalisateurs qui font des films similaires la même année
    if st.button("Créer les relations :CONCURRENCE entre réalisateurs"):
        msg = create_director_concurrence_relationships(driver)
        st.success(msg)

    # --- Collaborations fréquentes entre acteurs et réalisateurs ---
    # On importe la fonction qui récupère les collaborations réussies (fréquentes et efficaces)
    from database.neo4j import get_frequent_collaborations_with_success

    st.subheader("🎬 Collaborations fréquentes entre acteurs et réalisateurs avec succès (30)")

    # Lorsqu'on clique, on affiche les binômes acteur/réalisateur ayant eu plusieurs collaborations fructueuses
    if st.button("Afficher les collaborations fréquentes avec succès"):
        collaborations = get_frequent_collaborations_with_success(driver)
        if collaborations:
            for collab in collaborations:
                st.markdown(
                    f"- **{collab['actor']}** & **{collab['director']}** : {collab['collaborations']} collaborations – "
                    f"Revenu moyen : {collab['avg_revenue'] if collab['avg_revenue'] is not None else 'N/A'}M$ – "
                    f"Votes moyens : {collab['avg_votes'] if collab['avg_votes'] is not None else 'N/A'}"
                )
        else:
            st.warning("Aucune collaboration fréquente trouvée.")
