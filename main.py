import json
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from neo4j.exceptions import CypherSyntaxError 
import logging 




# --- Configuration de la Journalisation ---
try:
    from config import MONGODB_URI, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
except ImportError:
    st.error("ERREUR: Le fichier 'config.py' est manquant ou mal configuré.")
    st.stop() 

# --- Importations des modules de l'application ---
# (Les importations sont placées ici pour éviter les erreurs de dépendance circulaire)
try:
    from app.database.mongodb import MongoDBConnection, get_database, get_collection
    from app.database.neo4j import Neo4jConnection
    from app.queries.mongodb_queries import find_documents, insert_document, update_documents, delete_documents
    from app.queries.neo4j_queries import create_node, find_nodes
    from app.utils.visualizations import display_optimized_graph
    from app.queries.neo4j_queries import get_shortest_path_between_actors


except ImportError as e:
     st.warning(f"Attention: Importation échouée ({e}). Certaines fonctionnalités pourraient être indisponibles.")
     # Fournir des fonctions factices pour éviter les erreurs immédiates
     def MongoDBConnection(*args, **kwargs): raise NotImplementedError("MongoDBConnection non chargée")
     def Neo4jConnection(*args, **kwargs): raise NotImplementedError("Neo4jConnection non chargée")
     def get_database(*args, **kwargs): raise NotImplementedError("get_database non chargé")
     def get_collection(*args, **kwargs): raise NotImplementedError("get_collection non chargé")
     def find_documents(*args, **kwargs): raise NotImplementedError("find_documents non chargé")
     def insert_document(*args, **kwargs): raise NotImplementedError("insert_document non chargé")
     def update_documents(*args, **kwargs): raise NotImplementedError("update_documents non chargé")
     def delete_documents(*args, **kwargs): raise NotImplementedError("delete_documents non chargé")
     def create_node(*args, **kwargs): raise NotImplementedError("create_node non chargé")
     def find_nodes(*args, **kwargs): raise NotImplementedError("find_nodes non chargé")
     def display_optimized_graph(*args, **kwargs): st.error("display_optimized_graph non chargée.")


# --- Configuration de la Journalisation ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Fonction d'aide pour l'affichage des graphiques ---
def display_plot(fig):
    st.pyplot(fig)
    plt.clf()


# --- Logique Principale de l'Application ---
def main():
    st.set_page_config(page_title="Application NoSQL", layout="wide")
    st.title("Application NoSQL - MongoDB & Neo4j")

    tab_home, tab_mongo, tab_neo4j, tab_integrate, tab_analyze_viz = st.tabs([
        "Accueil", "MongoDB", "Neo4j", "Intégration", "Analyse & Visualisation"
    ])

    # --- Logique des Onglets ---
    with tab_home: show_home()
    with tab_mongo:
        try: show_mongodb_page()
        except NotImplementedError as e: st.warning(f"Fonctionnalité MongoDB non disponible: {e}")
        except Exception as e: st.error(f"Erreur MongoDB: {e}")
    with tab_neo4j:
        try: show_neo4j_page()
        except NotImplementedError as e: st.warning(f"Fonctionnalité Neo4j non disponible: {e}")
        except Exception as e: st.error(f"Erreur Neo4j: {e}")
    with tab_integrate: show_integration_page() 
    with tab_analyze_viz:
        try: show_analysis_visualization_page()
        except NotImplementedError as e: st.warning(f"Fonctionnalité Analyse/Viz non disponible: {e}")
        except Exception as e: st.error(f"Erreur Analyse/Viz: {e}")


# --- Définitions des Pages ---
def show_home():
    st.header("Bienvenue dans l'application NoSQL")
    st.markdown("""
        Interface pour MongoDB & Neo4j\n
        **Fonctionnalités Clés  :**
        *   Connexion sécurisée.
        *   CRUD MongoDB.
        *   Opérations Neo4j + Cypher perso.""")
    

# --- Page d'Analyse & Visualisation ---
def show_mongodb_page():
    st.header("Gestion MongoDB")


    try:
        with MongoDBConnection(MONGODB_URI) as client:
            st.success("Connexion MongoDB réussie")
            default_db = "Projet"
            default_coll = "movies"
            database_name = st.text_input("Base de données", default_db, key="mongodb_db_input")
            collection_name = st.text_input("Collection", default_coll, key="mongodb_collection_input")

            if not database_name or not collection_name:
                st.warning("Veuillez entrer un nom de base de données et de collection.")
                return

            # --- Opérations CRUD ---
            db = get_database(client, database_name)
            collection = get_collection(db, collection_name)
          

            col1, col2 = st.columns(2)
            # --- Rechercher & Insérer ---
            with col1:
                with st.expander("Rechercher des documents"):
                    query_str = st.text_area("Critères (JSON)", '{"year": 2006}', key="search_query")
                    limit = st.number_input("Limite", 1, 100, 10, key="search_limit")
                    if st.button("Rechercher", key="search_button"):
                        try:
                            query = json.loads(query_str)
                            results = find_documents(collection, query, limit=limit)
                            st.write(f"{len(results)} document(s) trouvé(s) :")
                            st.json(json.loads(json.dumps(results, default=str)))
                        except json.JSONDecodeError: st.error("JSON invalide (critères)")
                        except Exception as e: st.error(f"Erreur recherche: {e}")

                # --- Insérer un document ---
                with st.expander("Insérer un document"):
                    default_doc = '''{ "title": "Autre Film", "genre": "Comédie", "Director": "Réalisateur B", "Actors": "Acteur C", "year": 2024, "rating": "G", "Votes": 100 }'''
                    document_str = st.text_area("Document (JSON)", default_doc, key="insert_document", height=150)
                    if st.button("Insérer", key="insert_button"):
                        try:
                            document = json.loads(document_str)
                            result_id = insert_document(collection, document)
                            st.success(f"Document inséré avec ID: {result_id}")
                        except json.JSONDecodeError: st.error("JSON invalide (document)")
                        except Exception as e: st.error(f"Erreur insertion: {e}")

            # --- Mettre à jour & Supprimer ---
            with col2:
                with st.expander("Mettre à jour"):
                    query_str = st.text_area("Critères (JSON)", '{"title": "Autre Film"}', key="update_query")
                    update_str = st.text_area("Mise à jour (JSON)", '{"rating": "PG"}', key="update_data")
                    update_many = st.checkbox("MAJ multiple", key="update_many_check", value=False)
                    if st.button("Mettre à jour", key="update_button"):
                        try:
                            query = json.loads(query_str)
                            update = json.loads(update_str)
                            count = update_documents(collection, query, update, multi=update_many)
                            st.success(f"{count} document(s) mis à jour")
                        except json.JSONDecodeError: st.error("JSON invalide (critères/MAJ)")
                        except Exception as e: st.error(f"Erreur MAJ: {e}")

                # --- Supprimer des documents ---
                            
                with st.expander("Supprimer"):
                    query_str = st.text_area("Critères (JSON)", '{"_id": "100"}', key="delete_query")
                    delete_many = st.checkbox("Suppr. multiple", key="delete_many_check", value=False)
                    
                    # Gestion de la confirmation avec session_state
                    if 'delete_confirmed' not in st.session_state:
                        st.session_state.delete_confirmed = False
                        
                    confirmed = st.checkbox(
                        "Confirmer suppression ?", 
                        key="delete_confirm_checkbox",
                        value=st.session_state.delete_confirmed
                    )

                    # Bouton de suppression
                    if st.button("Supprimer", key="delete_button_primary", type="primary"):
                        if confirmed:
                            try:
                                query = json.loads(query_str)
                                
                                # Debug : Vérification de la requête
                                count_before = collection.count_documents(query)
                                st.write(f"Documents correspondants trouvés : {count_before}")
                                
                                # Appel à la fonction modifiée
                                count = delete_documents(collection, query, multi=delete_many)
                                
                                st.session_state.delete_confirmed = False
                                if count > 0:
                                    st.success(f"{count} document(s) supprimé(s)")
                                else:
                                    st.warning("Aucun document supprimé. Vérifiez les critères.")
                                    
                            except json.JSONDecodeError: 
                                st.error("Format JSON invalide")
                            except Exception as e: 
                                st.error(f"Erreur technique: {str(e)}")
                        else:
                            st.warning("Veuillez cocher la confirmation avant suppression")

                # --- Requêtes demandées dans le projet et affichage depuis streamlit ---
            st.subheader("Requêtes MongoDB")
            st.write("Cliquez sur un bouton pour exécuter la requête correspondante :")

            col1, col2, col3 = st.columns(3)

            # 1. Année avec le plus de films
            with col1:
                if st.button("1.Année avec le plus de films"):
                    result = collection.aggregate([
                        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}},
                        {"$limit": 1}
                    ])
                    for doc in result:
                        st.write(f"Année : {doc['_id']}, Nombre de films : {doc['count']}")
            
            # 2. Nombre de films après 1999
            with col1:
                if st.button("2.Films après 1999"):
                    count = collection.count_documents({"year": {"$gt": 1999}})
                    st.write(f"Nombre de films après 1999 : {count}")
            
            # 3. Moyenne des votes en 2007
            with col1:
                if st.button("3.Moyenne votes 2007"):
                    result = collection.aggregate([
                        {"$match": {"year": 2007}},
                        {"$group": {"_id": None, "averageVotes": {"$avg": "$votes"}}}
                    ])
                    for doc in result:
                        avg = doc.get("averageVotes")
                        if avg is not None:
                            st.write(f"Moyenne des votes en 2007 : {avg:.2f}")
                        else:
                            st.write("Aucune donnée pour les votes en 2007.")


            # 4. Histogramme des films par année
            with col1:
                if st.button("4.Histogramme films par année"):
                    data = collection.aggregate([
                        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
                        {"$sort": {"_id": 1}}
                    ])
                    years, counts = [], []
                    for doc in data:
                        if doc['_id']:  
                            years.append(doc['_id'])
                            counts.append(doc['count'])
                    df = pd.DataFrame({"Année": years, "Nombre de films": counts})
                    st.bar_chart(df.set_index("Année"))

            # 5. Genres disponibles
            with col2:
                if st.button("5.Genres disponibles"):
                    pipeline = [
                        {"$match": {"genre": {"$exists": True, "$ne": None}}},
                        {"$project": {
                            "genres": {
                                "$cond": {
                                    "if": {"$isArray": "$genre"},
                                    "then": "$genre",
                                    "else": {"$split": ["$genre", ","]}
                                }
                            }
                        }},
                        {"$unwind": "$genres"},
                        {"$project": {"genre": {"$trim": {"input": "$genres"}}}},
                        {"$group": {"_id": "$genre"}},
                        {"$sort": {"_id": 1}}
                    ]
                    result = list(collection.aggregate(pipeline))
                    genres = [doc['_id'] for doc in result]
                    st.write("Genres disponibles :", genres)

            # 6. Film avec le plus de revenu
            with col2:
                if st.button("6.Film plus rentable"):
                    result = collection.find(
                        {"Revenue (Millions)": {"$not": {"$eq": ""}}},
                        {"title": 1, "Revenue (Millions)": 1}
                    ).sort("Revenue (Millions)", -1).limit(1)
                    for doc in result:
                        st.write(f"Film : {doc['title']}, Revenu : {doc['Revenue (Millions)']}")
            
            # 7. Réalisateurs avec plus de 5 films
            with col2:
                if st.button("7.Réalisateurs avec plus de 5 films"):
                    pipeline = [
                        {"$group": {"_id": "$Director", "count": {"$count": {}}}},
                        {"$project": {"count": 1, "films": {"$gt": ["$count", 4]}}},
                        {"$match": {"films": True}}
                    ]
                    result = list(collection.aggregate(pipeline))
                    
                    if not result:
                        st.write("Aucun réalisateur trouvé avec plus de 5 films.")
                    else:
                        directors = [{"Réalisateur": doc['_id'], "Films": doc['count']} for doc in result]
                        st.dataframe(pd.DataFrame(directors))
            
            # 8. Quel est le genre de film qui rapporte en moyenne le plus de revenus ?
            with col2:
                if st.button("8.Genre le plus rentable"):
                    pipeline = [
                        {"$match": {"genre": {"$exists": True, "$ne": None}, "Revenue (Millions)": {"$ne": None}}},
                        {"$project": {
                            "genres": {
                                "$cond": {
                                    "if": {"$isArray": "$genre"},
                                    "then": "$genre",
                                    "else": {"$split": ["$genre", ","]}
                                }
                            },
                            "Revenue (Millions)": 1
                        }},
                        {"$unwind": "$genres"},
                        {"$project": {"genre": {"$trim": {"input": "$genres"}}, "Revenue (Millions)": 1}},
                        {"$group": {"_id": "$genre", "revenu_moyen": {"$avg": "$Revenue (Millions)"}}},
                        {"$sort": {"revenu_moyen": -1}},
                        {"$limit": 1} 
                    ]
                    result = list(collection.aggregate(pipeline))

                    if result:
                        st.dataframe(pd.DataFrame(result))
                    else:
                        st.write("Aucun genre trouvé.")

            # 9. Durée moyenne par décennie
            with col3:
                if st.button("9.Durée moyenne par décennie"):
                    pipeline = [
                        {"$addFields": {"numericMetascore": { "$convert": { "input": "$Metascore","to": "double", "onError": None,  #
                                        "onNull": None }}}},
                        { "$match": {"numericMetascore": {"$ne": None}} },
                        { "$addFields": {"decade": { "$subtract": [ "$year", {"$mod": ["$year", 10]}]}}},
                        {"$sort": {"numericMetascore": -1} },
                        { "$group": {"_id": "$decade", "filmsByDecade": { "$push": { "title": "$title","Metascore": "$numericMetascore",
                                        "year": "$year" }}}  },
                        { "$project": { "_id": 0,"decade": "$_id","top3Films": {"$slice": ["$filmsByDecade", 3]}  } },
                        { "$sort": {"decade": 1}
                        }
                    ]

                    results = list(collection.aggregate(pipeline))
                    st.write(results)

            # 10. Film le plus long par genre
            with col3:
                if st.button("10.Film le plus long par genre"):
                    pipeline = [
                        {
                            "$match": { "genre": {"$exists": True, "$ne": None}, "Runtime (Minutes)": {"$ne": None}, "title": {"$exists": True} 
                            }
                        },
                        {
                            "$project": { "title": 1, "Runtime (Minutes)": 1,
                                "genres": {
                                    "$cond": {
                                        "if": {"$isArray": "$genre"},
                                        "then": "$genre",
                                        "else": {"$split": ["$genre", ","]} 
                                    }
                                }
                            }
                        },
                        {"$unwind": "$genres"},
                        {
                            "$project": { "genre": {"$trim": {"input": "$genres"}},"title": 1,"Runtime (Minutes)": 1 }
                        },
                        {"$sort": {"Runtime (Minutes)": -1}},
                        {
                            "$group": {"_id": "$genre","Film le plus long": {"$first": "$title"}, "Durée (min)": {"$first": "$Runtime (Minutes)"}}
                        },
                        {"$sort": {"_id": 1}}
                    ]
                    result = list(collection.aggregate(pipeline))

                    if result:
                        df = pd.DataFrame(result)
                        df.rename(columns={"_id": "Genre"}, inplace=True)
                        st.dataframe(df[["Genre", "Film le plus long", "Durée (min)"]])
                    else:
                        st.write("Aucun film trouvé.")

            # 11. Créer une vue
            with col3:
                if st.button("11.Créer vue films >80 & >50Millions"):
                    try:
                        client[database_name].command({
                            "create": "highMovies",
                            "viewOn": collection_name,
                            "pipeline": [{"$match": {"Metascore": {"$gt": 80}, "Revenue (Millions)": {"$gt": 50000000}}}]
                        })
                        st.success("Vue 'highMovies' créée avec succès.")
                    except Exception as e:
                        st.error(f"Erreur lors de la création de la vue : {e}")

            # 12. Mettre à jour les films avec un Metascore > 80
                if st.button("12.corrélation entre la durée des films "):
                    data = list(collection.find(
                        {
                            "Runtime (Minutes)": {"$exists": True, "$type": "number"},
                            "Revenue (Millions)": {"$exists": True, "$type": "number"}
                        },
                        {
                            "Runtime (Minutes)": 1,
                            "Revenue (Millions)": 1
                        }
                    ))
                    if not data:
                        return None
                    df = pd.DataFrame(data)

                    # On renomme pour simplifier
                    df.rename(columns={
                        "Runtime (Minutes)": "runtime",
                        "Revenue (Millions)": "revenue"
                    }, inplace=True)
                    st.write("La correlation est de : ", df["runtime"].corr(df["revenue"])) 
            
            # 13. Evolution de la durée des films
            with col3:
                if st.button("13.Evolution de la durée des films"):
                    pipeline = [
                    { "$addFields": {"numericRuntime": {"$convert": {"input": "$Runtime (Minutes)", "to": "double",
                                    "onError": None, 
                                    "onNull": None }}}},
                    {"$match": { "numericRuntime": {"$ne": None}} },
                
                    {"$addFields": { "decade": {"$subtract": ["$year", {"$mod": ["$year", 10]}]}}},
                    {"$group": {"_id": "$decade", "avgRuntime": {"$avg": "$numericRuntime"}}},
                    {"$project": {"_id": 0, "decade": "$_id", "avgRuntime": 1}},
                    {"$sort": {"_id": 1}}
                ]
                    st.write(list(collection.aggregate(pipeline)))
           
    # Gestion des erreurs
    except NotImplementedError as e:
         st.warning(f"Fonctionnalité MongoDB non disponible: {e}")
    except Exception as e:
        st.error(f"Erreur connexion MongoDB: {e}")
        st.warning("Vérifiez `MONGODB_URI` et accès réseau.")

# --- Page d'Analyse & Visualisation ---
def show_neo4j_page():
    st.header("Gestion Neo4j")

    # --- Connexion & Opérations Neo4j ---
    try:
        with Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) as driver:
            st.success("Connexion Neo4j réussie")
            col1, col2 = st.columns(2)

            # --- Créer Nœud & Relation ---
            with col1:
                with st.expander("Créer un nœud"):
                    label = st.text_input("Label", "Person", key="neo4j_label_input")
                    props = st.text_area("Propriétés (JSON)", '{"name": "Bob", "city": "Paris"}', key="create_node_props")
                    if st.button("Créer Nœud", key="create_node_button"):
                        if label and props:
                            try: create_node(driver.session(), label, json.loads(props)); st.success("Nœud créé")
                            except json.JSONDecodeError: st.error("JSON Propriétés invalide")
                            except Exception as e: st.error(f"Erreur création nœud: {e}")
                        else: st.warning("Label et Propriétés requis.")

                # --- Créer Relation ---
                with st.expander("Créer une relation"):
                    st.text("Identifier les nœuds par label/propriété:")
                    c1, c2 = st.columns(2)
                    with c1:
                        sl = st.text_input("Label Départ", "Person", key="sl"); sk = st.text_input("Prop Clé Départ", "name", key="sk"); sv = st.text_input("Prop Val Départ", "Bob", key="sv")
                    with c2:
                        el = st.text_input("Label Arrivée", "City", key="el"); ek = st.text_input("Prop Clé Arrivée", "name", key="ek"); ev = st.text_input("Prop Val Arrivée", "Paris", key="ev")
                    rt = st.text_input("Type Relation", "LIVES_IN", key="rt") 
                    rp = st.text_area("Propriétés Relation (JSON)", '{}', key="rp")
                    if st.button("Créer Relation", key="cr_btn"):
                        if all([sl, sk, sv, el, ek, ev, rt]) and rt.replace('_','').isalnum():
                            try:
                                r_props = json.loads(rp)
                                with driver.session() as s:
                                    q_check = f"MATCH (a:{sl} {{{sk}: $sv}}), (b:{el} {{{ek}: $ev}}) RETURN count(a) AS ca, count(b) AS cb"
                                    chk = s.run(q_check, sv=sv, ev=ev).single()
                                    if chk and chk["ca"] > 0 and chk["cb"] > 0:
                                        q_cr = f"MATCH (a:{sl} {{{sk}: $sv}}), (b:{el} {{{ek}: $ev}}) MERGE (a)-[r:`{rt}`]->(b) SET r += $rp RETURN count(r)"
                                        cnt = s.run(q_cr, sv=sv, ev=ev, rp=r_props).single()[0]
                                        st.success(f"Relation créée/MAJ ({cnt})")
                                    else: st.warning("Nœud(s) de départ/arrivée non trouvés.")
                            except json.JSONDecodeError: st.error("JSON Props Relation invalide")
                            except Exception as e: st.error(f"Erreur création relation: {e}")
                        else: st.warning("Infos/Type relation invalides.")

            # ---  Rechercher Nœuds & Exécuter Cypher ---
            with col2:
                with st.expander("Rechercher des nœuds"):
                    lbl = st.text_input("Label (optionnel)", key="sn_lbl")
                    lim = st.number_input("Limite", 1, 100, 10, key="sn_lim")
                    if st.button("Rechercher Nœuds", key="sn_btn"):
                        try:
                             res = find_nodes(driver.session(), label=lbl if lbl else None, limit=lim)
                             st.write(f"{len(res)} nœud(s) trouvé(s):"); st.json(res)
                        except Exception as e: st.error(f"Erreur recherche nœuds: {e}")

                # --- Exécuter Cypher ---
                with st.expander("Exécuter Cypher"):
                    cq = st.text_area("Requête Cypher", "MATCH (n) RETURN count(n)", key="cq", height=100)
                    cp_str = st.text_area("Paramètres (JSON)", '{}', key="cp")
                    if st.button("Exécuter", key="run_cq"):
                        if cq:
                            try:
                                cp = json.loads(cp_str)
                                with driver.session() as s:
                                    res = s.run(cq, parameters=cp)
                                    try:
                                        data = res.data()
                                        if data: st.write("Résultats:"); st.dataframe(pd.DataFrame(data))
                                        else: st.write("Résumé:"); st.json(dict(res.consume().counters))
                                    except Exception as de: # Fallback si .data() échoue
                                         st.warning(f"Affichage tableau échoué ({de}). Données brutes:")
                                         st.json([r.data() for r in res]) # Tenter de lire le reste
                            except json.JSONDecodeError: st.error("JSON Paramètres invalide")
                            except Exception as e: st.error(f"Erreur exécution Cypher: {e}")
                        else: st.warning("Requête vide.")
                
            st.header("Requêtes")
            st.write("Cliquez sur un bouton pour exécuter la requête correspondante :")

            col_btn1, col_btn2, col_btn3 = st.columns(3)
            with driver.session() as session:

                # Question 14 acteur avec le plus de films
                with col_btn1:
                    if st.button("14.Acteur avec le plus de films"):
                        result = session.run("""
                            MATCH (a:Actor)-[:ACTED_IN]->(f:Film)
                            WITH a, COUNT(f) AS filmCount
                            RETURN a.name, filmCount
                            ORDER BY filmCount DESC
                            LIMIT 1
                        """)
                        record = result.single()
                        st.write(f"Acteur : {record['a.name']}, Nombre de films : {record['filmCount']}")

                # Question 15 acteurs ayant joué avec Anne Hathaway
                with col_btn1:
                    if st.button("15.Co-acteurs d'Anne Hathaway"):
                        result = session.run("""
                            MATCH (ah:Actor {name: "Anne Hathaway"})-[:ACTED_IN]->(f:Film)<-[:ACTED_IN]-(a:Actor)
                            WHERE a <> ah
                            RETURN DISTINCT a.name
                        """)
                        actors = [record["a.name"] for record in result]
                        st.write("Acteurs ayant joué avec Anne Hathaway :", actors)

                # Question 16 acteur avec le plus de revenus
                with col_btn1:
                    if st.button("16.Acteur avec le plus de revenus"):
                        result = session.run("""
                            MATCH (a:Actor)-[:ACTED_IN]->(f:Film)
                            WHERE f.revenue IS NOT NULL
                            WITH a, SUM(f.revenue) AS totalRevenue
                            RETURN a.name, totalRevenue
                            ORDER BY totalRevenue DESC
                            LIMIT 1
                        """)
                        record = result.single()
                        st.write(f"Acteur : {record['a.name']}, Revenus totaux : {record['totalRevenue']}")

                # Question 17 moyenne des votes
                with col_btn1:
                    if st.button("17.Moyenne des votes"):
                        result = session.run("""
                            MATCH (f:Film)
                            WHERE f.votes IS NOT NULL
                            RETURN AVG(f.votes) AS averageVotes
                        """)
                        record = result.single()
                        st.write(f"Moyenne des votes : {record['averageVotes']:.2f}")

                # Question 18 genre le plus représenté
                with col_btn2:
                    if st.button("18.Genre le plus représenté"):
                        result = session.run("""
                            MATCH (g:Genre)<-[:HAS_GENRE]-(f:Film)
                            WITH g, COUNT(f) AS filmCount
                            RETURN g.name, filmCount
                            ORDER BY filmCount DESC
                            LIMIT 1
                        """)
                        record = result.single()
                        st.write(f"Genre : {record['g.name']}, Nombre de films : {record['filmCount']}")

                # Question 19 les films dans lesquels les acteurs ayant jou´e avec vous ont ´egalement jou´e 
                with col_btn2:
                    if st.button("19.Voir les films des co-acteurs des membres"):
                        with driver.session() as session:
                            result = session.run("""
                                MATCH (member:Actor)-[:PART_OF_PROJECT_TEAM]->(:Film)
                                WITH collect(member) AS members
                                MATCH (member:Actor)-[:ACTED_IN]->(f1:Film)<-[:ACTED_IN]-(coactor:Actor)-[:ACTED_IN]->(f2:Film)
                                WHERE member IN members AND f1 <> f2 AND NOT (member)-[:ACTED_IN]->(f2)
                                RETURN DISTINCT f2.title
                            """)
                            films = [record["f2.title"] for record in result]
                            st.write("Films où les co-acteurs des membres du projet ont joué :", films)

                # Question 20 réalisateur Director a travaillé avec le plus grand nombre d’acteurs
                with col_btn2:
                    if st.button("20.Réalisateur avec le plus d'acteurs"):
                        result = session.run("""
                            MATCH (d:Director)-[:DIRECTED]->(f:Film)<-[:ACTED_IN]-(a:Actor)
                            WITH d, COLLECT(DISTINCT a) AS actors
                            RETURN d.name, SIZE(actors) AS actorCount
                            ORDER BY actorCount DESC
                            LIMIT 1
                        """)
                        record = result.single()
                        st.write(f"Réalisteur : {record['d.name']}, Nombre d'acteurs : {record['actorCount']}")

                # Question 21 les films les plus ”connect´es”
                with col_btn2:
                    if st.button("21.Films les plus connectés"):
                        result = session.run("""
                            MATCH (f1:Film)<-[:ACTED_IN]-(a:Actor)-[:ACTED_IN]->(f2:Film)
                            WHERE f1 <> f2
                            WITH f1, COUNT(DISTINCT f2) AS Occurences
                            RETURN f1.title, Occurences 
                            ORDER BY Occurences DESC
                            LIMIT 5
                        """)
                        st.dataframe(pd.DataFrame(result.data()))

                # Question 22 - 5 acteurs ayant jou´e avec le plus de r´ealisateurs différents
                with col_btn3:
                    if st.button("22.Top5 acteurs avec réalisateurs"):
                        result = session.run("""
                            MATCH (a:Actor)-[:ACTED_IN]->(f:Film)<-[:DIRECTED]-(d:Director)
                            WITH a, COLLECT(DISTINCT d) AS directors
                            RETURN a.name, SIZE(directors) AS directorCount
                            ORDER BY directorCount DESC
                            LIMIT 5
                        """)
                        st.dataframe(pd.DataFrame(result.data()))

                # Question 23 Recommander un film à un acteur en fonction des genres des films o`u il a déjà joué.b
                with col_btn3:
                    actor_name = st.text_input("Enter name actor", "Tom Hanks", key="actor_23")
                    if st.button("23.Recommander un film"):
                        result = session.run("""
                            MATCH (a:Actor {name: $actor_name})-[:ACTED_IN]->(f1:Film)-[:HAS_GENRE]->(g:Genre)
                            WITH a, COLLECT(DISTINCT g) AS genres
                            MATCH (f2:Film)-[:HAS_GENRE]->(g:Genre)
                            WHERE NOT (a)-[:ACTED_IN]->(f2) AND g IN genres
                            RETURN f2.title, COLLECT(g.name) AS genres
                            ORDER BY SIZE(COLLECT(g.name)) DESC
                            LIMIT 1
                        """, {"actor_name": actor_name})
                        record = result.single()
                        if record:
                            st.write(f"Film recommandé pour {actor_name} : {record['f2.title']}, Genres : {record['genres']}")
                        else:
                            st.write(f"Aucun film recommandé trouvé pour {actor_name}.")

                # Question 24 : Créer INFLUENCE_PAR entre les réalisateurs
                with col_btn3:
                    if st.button("24.Relation entre les réalisateurs"):
                        try:
                            session.run("""
                                MATCH (d1:Director)-[:DIRECTED]->(f1:Film)-[:HAS_GENRE]->(g:Genre)<-[:HAS_GENRE]-(f2:Film)<-[:DIRECTED]-(d2:Director)
                                WHERE d1 <> d2
                                WITH d1, d2, COLLECT(DISTINCT g) AS commonGenres
                                WHERE SIZE(commonGenres) > 0
                                MERGE (d1)-[:INFLUENCE_PAR {genres: [genre IN commonGenres | genre.name]}]->(d2)
                            """)
                            st.success("Relations INFLUENCE_PAR créées entre réalisateurs.")
                        except Exception as e:
                            st.error(f"Erreur Neo4j: {e}")
                
                # Question 25 : Chemin le plus court entre deux acteurs
                actor1 = st.text_input("Acteur 1", "Tom Hanks", key="actor1")
                actor2 = st.text_input("Acteur 2", "Scarlett Johansson", key="actor2")
                if st.button("25.Trouver le plus court chemin", key="find_path"):
                    result = get_shortest_path_between_actors(session, actor1, actor2)
                    if result:
                        st.write("Chemin trouvé :")
                        path_str = ""
                        for i, node in enumerate(result["nodes"]):
                            node_type = node["type"]
                            node_name = node["properties"].get("name", node["properties"].get("title", "Inconnu"))
                            path_str += f"{node_type}: {node_name}"
                            if i < len(result["relationships"]):
                                path_str += f" --[{result['relationships'][i]}]--> "
                        st.write(path_str)
                    else:
                        st.write(f"Aucun chemin trouvé entre {actor1} et {actor2}.")

    except NotImplementedError as e:
        st.warning(f"Fonctionnalité Neo4j non disponible: {e}")
    except Exception as e:
        st.error(f"Erreur connexion Neo4j: {e}")
        st.warning("Vérifiez `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` et accès réseau.")


# --- Page d'Intégration ---
def show_integration_page():
    st.header("Intégration MongoDB -> Neo4j")
    

    # --- Logique d'Intégration ---
    default_db = "Projet"
    default_coll = "movies"
    mongo_db_name = st.text_input("Base MongoDB source", default_db, key="mongo_db_input_neo4j")
    mongo_collection_name = st.text_input("Collection MongoDB source", default_coll, key="mongo_collection_input_neo4j")

    # --- Intégration Spécifique ---
    st.subheader("Ajout des Membres du Projet")
    project_members_str = st.text_area("Noms des membres (un par ligne)", "Membre Equipe 1\nMembre Equipe 2", key="proj_members")
    target_film_title = st.text_input("Titre du film auquel lier les membres", "The Departed", key="proj_film")

    st.warning("L'intégration modifiera votre base Neo4j et peut prendre du temps.")

    # --- Bouton d'Intégration ---
    if st.button("Lancer l'intégration (Exigences Spécifiques)", key="integrate_specific_button"):
        if not mongo_db_name or not mongo_collection_name:
            st.error("Veuillez spécifier la base et la collection MongoDB.")
            return
        if not target_film_title:
            st.error("Veuillez spécifier un titre de film pour lier les membres du projet.")
            return

        # Extraction des noms des membres du projet
        project_members = [name.strip() for name in project_members_str.split('\n') if name.strip()]
        if not project_members:
            st.warning("Aucun nom de membre de projet valide fourni. Étape ignorée.")

        # Lancer l'intégration
        try:
            integrate_mongodb_to_neo4j_specific(
                mongo_db_name,
                mongo_collection_name,
                project_members,
                target_film_title
            )
            st.success("Intégration spécifique terminée avec succès!")
        except NotImplementedError as e:
             st.error(f"Erreur: Fonction non implémentée: {e}")
        except Exception as e:
            st.error(f"Erreur lors de l'intégration: {str(e)}")
            logging.exception("Erreur détaillée d'intégration:")


#fonction pour intégrer les données MongoDB vers Neo4j
def integrate_mongodb_to_neo4j_specific(
    mongo_db_name: str,
    mongo_collection_name: str,
    project_members: list[str],
    target_film_title: str
    ):
    # --- Logique d'Intégration Spécifique ---
    processed_count = 0
    skipped_count = 0
    batch_size = 250
    neo4j_db = "neo4j"
    revenue_field_hardcoded = "Revenue (Millions)" 

    status_placeholder = st.empty()

    try:
        with Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) as driver:
            logging.info(f"Connecté à Neo4j ({NEO4J_URI}) pour l'intégration.")

            #fonction pour traiter un lot de films pour créer/mettre à jour des nœuds et relations dans Neo4j
            with driver.session(database=neo4j_db) as session:
                try: # Création des contraintes
                    session.run("CREATE CONSTRAINT constraint_film_mongoId IF NOT EXISTS FOR (f:Film) REQUIRE f.mongoId IS UNIQUE")
                    session.run("CREATE CONSTRAINT constraint_actor_name IF NOT EXISTS FOR (a:Actor) REQUIRE a.name IS UNIQUE")
                    session.run("CREATE CONSTRAINT constraint_director_name IF NOT EXISTS FOR (d:Director) REQUIRE d.name IS UNIQUE")
                    session.run("CREATE CONSTRAINT constraint_genre_name IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE")

                except Exception as e:
                    logging.warning(f"Échec contraintes Neo4j: {e}")
                    st.warning(f"Avertissement contraintes Neo4j (voir logs).")

            # --- Extraction & Transformation des Données ---
            with MongoDBConnection(MONGODB_URI) as mongo_client:
                logging.info(f"Connecté à MongoDB ({MONGODB_URI}).")
                db = get_database(mongo_client, mongo_db_name)
                collection = get_collection(db, mongo_collection_name)

                # --- Préparation de la Requête ---
                query_filter = {"_id": {"$not": {"$regex": "^_design/"}}}
                projection = {
                    "_id": 1, "title": 1, "year": 1, "Votes": 1, "rating": 1,
                    "Director": 1, "Actors": 1, "genre": 1,
                    revenue_field_hardcoded: 1 
                }

                # Estimation du nombre de documents
                total_docs_estimated = collection.count_documents(query_filter)
                logging.info(f"Estimation de {total_docs_estimated} documents à traiter...")
                films_cursor = collection.find(query_filter, projection)

                batch = []
                status_placeholder.status(f"Traitement de {total_docs_estimated} documents...", expanded=True)

                # --- Boucle de Traitement ---
                for i, film_doc in enumerate(films_cursor):
                    # --- Validation et Nettoyage ---
                    mongo_id_obj = film_doc.get("_id")
                    if isinstance(mongo_id_obj, str) and mongo_id_obj.startswith("_design/"):
                         skipped_count += 1; continue # Ignorer design docs

                    mongo_id = str(mongo_id_obj) if mongo_id_obj else None
                    title = film_doc.get("title")
                    if not title or not mongo_id:
                        logging.warning(f"Doc manquant 'title'/'_id': {mongo_id}. Ignoré."); skipped_count += 1; continue

                    # Conversion des champs
                    try: year = int(film_doc.get("year")) if film_doc.get("year") is not None else None
                    except: year = None

                    try: votes = int(film_doc.get("Votes")) if film_doc.get("Votes") is not None else None
                    except: votes = None

                    # Nettoyage des champs
                    rating_classification = film_doc.get("rating")
                    if not isinstance(rating_classification, str) or rating_classification == '': rating_classification = None
                    directors_raw = film_doc.get("Director", [])
                    directors_list = []

                    if isinstance(directors_raw, str): directors_list = [d.strip() for d in directors_raw.split(',') if d.strip()]
                    elif isinstance(directors_raw, list): directors_list = [str(d).strip() for d in directors_raw if str(d).strip()]

                    first_director = directors_list[0] if directors_list else None
                    actors_str = film_doc.get("Actors", "")
                    actors_list = [a.strip() for a in actors_str.split(',') if a.strip()] if isinstance(actors_str, str) else []
                    genres_str = film_doc.get("genre", "")
                    genres_list = [g.strip() for g in genres_str.split(',') if g.strip()] if isinstance(genres_str, str) else []

                    
                    revenue_raw = film_doc.get(revenue_field_hardcoded)
                    revenue = None
                    # Conversion du revenu en nombre
                    try: 
                         if isinstance(revenue_raw, (int, float)): revenue = revenue_raw
                         elif isinstance(revenue_raw, str):
                             revenue_cleaned = revenue_raw.replace('$', '').replace(',', '')
                             multiplier = 1
                             if 'M' in revenue_cleaned.upper(): multiplier = 1_000_000; revenue_cleaned = revenue_cleaned.upper().replace('M', '')
                             elif 'K' in revenue_cleaned.upper(): multiplier = 1_000; revenue_cleaned = revenue_cleaned.upper().replace('K', '')
                             revenue = float(revenue_cleaned) * multiplier
                    except (ValueError, TypeError): revenue = None

                    # --- Préparation des Données pour Neo4j ---
                    film_data_for_neo4j = {
                        "mongoId": mongo_id, "title": title, "year": year, "votes": votes,
                        "rating": rating_classification, "director": first_director, "revenue": revenue,
                        "allDirectors": directors_list, "actors": actors_list, "genres": genres_list
                    }
                    batch.append(film_data_for_neo4j)

                    # --- Exécuter le lot ---
                    if len(batch) >= batch_size or (total_docs_estimated > 0 and i == total_docs_estimated - 1):
                        #fonction pour traiter un lot de films pour créer/mettre à jour des nœuds et relations dans Neo4j
                        try:
                            with driver.session(database=neo4j_db) as session:
                                session.execute_write(process_film_batch_specific, batch)
                            processed_count += len(batch)
                            status_placeholder.info(f"Lot traité. Total : {processed_count}/{total_docs_estimated}")
                            batch = []
                        except Exception as e:
                            error_msg = f"Erreur traitement lot Neo4j: {e}"
                            status_placeholder.error(error_msg)
                            logging.error(error_msg, exc_info=True)
                            return # Arrêter le traitement en cas d'erreur

                # --- Finalisation ---
                final_message = f"Intégration terminée. {processed_count} films traités, {skipped_count} ignorés."
                status_placeholder.success(final_message)
                logging.info(final_message)

            # --- Ajout Membres Projet ---
            if project_members and target_film_title:
                 
                #Logique ajout membres du projet
                 status_members = st.status(f"Ajout membres '{target_film_title}'...")
                 try:
                     with driver.session(database=neo4j_db) as session:
                         session.execute_write(add_project_members_to_film, project_members, target_film_title)
                     status_members.update(label="Membres projet ajoutés.", state="complete")
                 except Exception as e:
                     status_members.update(label=f"Erreur ajout membres: {e}", state="error")
                     logging.error(f"Erreur ajout membres: {e}", exc_info=True)
            else: st.info("Ajout membres projet ignoré.")

    # --- Gestion des Erreurs ---
    except NotImplementedError as e:
         st.error(f"Erreur: Fonction non implémentée: {e}")
         logging.error(f"Erreur NotImplementedError: {e}")
    except Exception as e:
        error_msg_global = f"Erreur inattendue intégration: {e}"
        try: status_placeholder.error(error_msg_global)
        except NameError: st.error(error_msg_global)
        logging.exception("Erreur inattendue hors lot:")

# --- Fonction de Transaction Neo4j ---
def process_film_batch_specific(tx, films_batch):
    """
    Traite un lot de films pour créer/mettre à jour des nœuds et relations dans Neo4j.
    """
    # --- Préparation des Clauses SET ---
    set_clause_parts = [
        "f.title = film_data.title", "f.year = film_data.year", "f.votes = film_data.votes",
        "f.rating = film_data.rating", "f.director = film_data.director",
        "f.revenue = film_data.revenue" 
    ]
    set_clause = ", ".join(set_clause_parts)

    # Requête Cypher pour traiter le lot
    query = f"""
    UNWIND $films AS film_data

    //Fusionner Film
    MERGE (f:Film {{mongoId: film_data.mongoId}})
    ON CREATE SET f.mongoId = film_data.mongoId, {set_clause}
    ON MATCH SET {set_clause}

    //Genres + HAS_GENRE
    WITH f, film_data
    FOREACH (genre_name IN [g IN film_data.genres WHERE g IS NOT NULL AND g <> ''] |
        MERGE (g_node:Genre {{name: genre_name}})
        MERGE (f)-[:HAS_GENRE]->(g_node)
    )

    //Actors + ACTED_IN
    WITH f, film_data
    CALL {{
        WITH film_data
        UNWIND [a IN film_data.actors WHERE a IS NOT NULL AND a <> ''] AS actor_name
        MERGE (a_node:Actor {{name: actor_name}})
        RETURN collect(a_node) AS filmActors
    }}
    FOREACH (actor IN filmActors | MERGE (actor)-[:ACTED_IN]->(f) )

    //Directors + DIRECTED + WORKED_WITH
    WITH f, film_data, filmActors
    CALL {{
        WITH film_data
        UNWIND [d IN film_data.allDirectors WHERE d IS NOT NULL AND d <> ''] AS director_name
        MERGE (d_node:Director {{name: director_name}})
        RETURN collect(d_node) AS filmDirectors
    }}
    FOREACH (director IN filmDirectors |
        MERGE (director)-[:DIRECTED]->(f)
        FOREACH (actor IN filmActors |
            MERGE (director)-[:WORKED_WITH]->(actor)
        )
    )
    """
    tx.run(query, films=films_batch)



# --- Fonction de Transaction des membres du projet-film -> Neo4j ---
def add_project_members_to_film(tx, member_names, film_title):
    query = """
    MATCH (f:Film {title: $film_title}) WITH f
    UNWIND $member_names AS member_name
    MERGE (p:Actor {name: member_name})
    MERGE (p)-[:PART_OF_PROJECT_TEAM]->(f)
    """
    result = tx.run(query, member_names=member_names, film_title=film_title)
    counters = result.consume().counters
    counters_dict = {
        "nodes_created": counters.nodes_created,
        "relationships_created": counters.relationships_created,
        "nodes_deleted": counters.nodes_deleted,
        "relationships_deleted": counters.relationships_deleted,
        "properties_set": counters.properties_set
    }
    logging.info(f"Résumé liaison membres projet: {counters_dict}")


# --- Page d'Analyse et Visualisation ---
def show_analysis_visualization_page():
    st.header("Analyse & Visualisation")
    viz_type = st.selectbox("Source de données", ["MongoDB", "Neo4j"])
    neo4j_db = "neo4j"

    # ================== MongoDB ==================
    # --- Analyse MongoDB ---
    if viz_type == "MongoDB":
        st.subheader("Analyse MongoDB")
        try:
            with MongoDBConnection(MONGODB_URI) as client:
                default_db = "Projet"; default_coll = "movies"
                db_name = st.text_input("Base MongoDB", default_db, key="mdb_viz_db")
                coll_name = st.text_input("Collection MongoDB", default_coll, key="mdb_viz_coll")
                if not db_name or not coll_name: st.warning("Spécifiez Base/Collection."); return

                collection = get_collection(get_database(client, db_name), coll_name)
                st.info(f"Analyse de `{db_name}.{coll_name}`")

                opts = ["Distr. Metascore", "Distr. Classifications", "Films/An", "Top Genres", "Metascore vs Votes"]
                analysis = st.selectbox("Analyse", opts)

                # Metascore
                if analysis == opts[0]:
                    with st.spinner("Chargement Metascore..."):
                        data = list(collection.find({"Metascore": {"$exists":1, "$ne":None, "$ne":""}}, {"_id":0, "Metascore":1}))
                    if data:
                        df = pd.DataFrame(data)
                        df['Metascore'] = pd.to_numeric(df['Metascore'], errors='coerce')
                        df.dropna(inplace=True)
                        if not df.empty:
                            fig, ax = plt.subplots(); sns.histplot(df['Metascore'], kde=True, ax=ax, bins=20)
                            ax.set_title("Distribution Metascore"); ax.set_xlabel("Metascore"); ax.set_ylabel("# Films")
                            display_plot(fig)
                        else: st.warning("Pas de Metascore numérique valide.")
                    else: st.warning("Champ 'Metascore' non trouvé.")

                # Classifications
                elif analysis == opts[1]:
                     with st.spinner("Chargement Classifications..."):
                         p = [{"$match": {"rating": {"$exists":1, "$ne":None, "$ne":""}}}, {"$group": {"_id":"$rating", "count":{"$sum":1}}}, {"$sort":{"count":-1}}]
                         data = list(collection.aggregate(p))
                     if data:
                         df = pd.DataFrame(data).rename(columns={"_id":"cls", "count":"n"})
                         fig, ax = plt.subplots(figsize=(8, max(5, len(df)*0.5)))
                         sns.barplot(data=df, x='n', y='cls', ax=ax, palette="coolwarm")
                         ax.set_title("Films par Classification"); ax.set_xlabel("# Films"); ax.set_ylabel("Classification")
                         plt.tight_layout(); display_plot(fig)
                     else: st.warning("Champ 'rating' non trouvé.")

                # Films/An
                elif analysis == opts[2]:
                     with st.spinner("Chargement Films/An..."):
                          p = [{"$addFields": {"year_num": {"$toInt": "$year"}}}, {"$match": {"year_num": {"$exists":1, "$ne":None}}},
                               {"$group": {"_id":"$year_num", "count":{"$sum":1}}}, {"$sort":{"_id":1}}]
                          data = list(collection.aggregate(p))
                     if data:
                         df = pd.DataFrame(data).rename(columns={"_id":"year"})
                         df = df[(df['year'] > 1900) & (df['year'] <= pd.Timestamp.now().year)]
                         if not df.empty:
                             fig, ax = plt.subplots(figsize=(12,6)); sns.lineplot(data=df, x='year', y='count', ax=ax, marker='o')
                             ax.set_title("Films par An"); ax.set_xlabel("Année"); ax.set_ylabel("# Films")
                             plt.xticks(rotation=45, ha='right'); plt.tight_layout(); display_plot(fig)
                         else: st.warning("Pas d'années valides (1901-présent).")
                     else: st.warning("Champ 'year' non trouvé/convertible.")

                # Top Genres
                elif analysis == opts[3]:
                     with st.spinner("Analyse Genres..."):
                          data = list(collection.find({"genre":{"$exists":1, "$ne":None, "$ne":""}}, {"_id":0, "genre":1}))
                     if data:
                          try:
                              genres = pd.DataFrame(data)['genre'].str.split(',').explode().str.strip()
                              counts = genres.value_counts().head(15)
                              if not counts.empty:
                                  df = counts.reset_index().set_axis(['genre','count'], axis=1)
                                  fig, ax = plt.subplots(figsize=(10,7)); sns.barplot(data=df, x='count', y='genre', ax=ax, palette='viridis')
                                  ax.set_title("Top Genres"); ax.set_xlabel("# Films"); ax.set_ylabel("Genre")
                                  plt.tight_layout(); display_plot(fig)
                              else: st.warning("Aucun genre trouvé après traitement.")
                          except Exception as e: st.error(f"Erreur traitement genres: {e}")
                     else: st.warning("Champ 'genre' non trouvé.")

                # Metascore vs Votes
                elif analysis == opts[4]:
                     with st.spinner("Chargement Metascore/Votes..."):
                          data = list(collection.find({"Metascore":{"$exists":1, "$ne":None, "$ne":""}, "Votes":{"$exists":1, "$ne":None}},
                                                     {"_id":0, "Metascore":1, "Votes":1}).limit(2000))
                     if data:
                          df = pd.DataFrame(data)
                          df['Metascore'] = pd.to_numeric(df['Metascore'], errors='coerce')
                          df['Votes'] = pd.to_numeric(df['Votes'], errors='coerce')
                          df.dropna(inplace=True)
                          if not df.empty:
                              fig, ax = plt.subplots(figsize=(10,6)); sns.scatterplot(data=df, x='Votes', y='Metascore', ax=ax, alpha=0.5)
                              ax.set_title("Metascore vs Votes"); ax.set_xlabel("Votes"); ax.set_ylabel("Metascore")
                              if df['Votes'].max() / max(1, df['Votes'].min()) > 100: ax.set_xscale('log'); ax.set_xlabel("Votes (log)")
                              plt.tight_layout(); display_plot(fig)
                          else: st.warning("Pas de données valides Metascore/Votes.")
                     else: st.warning("Champs 'Metascore'/'Votes' non trouvés.")

        # --- Gestion des Erreurs ---
        except NotImplementedError as e: st.warning(f"Fonctionnalité MongoDB non disponible: {e}")
        except Exception as e: st.error(f"Erreur Analyse MongoDB: {e}"); st.exception(e)

    # ================== Neo4j ==================
    elif viz_type == "Neo4j":
        st.subheader("Visualisation Graphe Neo4j")
        try:
            with Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) as driver:
                st.info("Connecté à Neo4j.")
                col1, col2 = st.columns(2)
                with col1: limit = st.number_input("Limite Nœuds", 10, 5000, 150, key="viz_lim")
                with col2:
                    try:
                        with driver.session(database=neo4j_db) as s: lbls = [r["label"] for r in s.run("CALL db.labels() YIELD label RETURN label")]
                        defaults = [l for l in ["Film", "Actor", "Director", "Genre"] if l in lbls]
                    except Exception as e: lbls = ["Film", "Actor", "Director", "Genre"]; defaults = ["Film", "Actor"]; st.warning(f"Labels non récupérés ({e})")
                    sel_lbls = st.multiselect("Filtrer Labels", lbls, default=defaults, key="viz_lbls")

                physics = st.checkbox("Physique", True, key="viz_phy")
                if st.button("Afficher Graphe", key="viz_btn"):
                    with st.spinner("Construction graphe..."):
                        try:
                            filt = ""; params = {"limit": limit, "labels": sel_lbls if sel_lbls else None}
                            if sel_lbls: filt = "WHERE size([l IN labels(n) WHERE l IN $labels]) > 0"

                            q = f"MATCH (n) {filt} WITH n LIMIT $limit OPTIONAL MATCH (n)-[r]-(m) WHERE $labels IS NULL OR size([l IN labels(m) WHERE l IN $labels]) > 0 RETURN n, r, m"
                            res_tup = driver.execute_query(q, params, database_=neo4j_db)
                            recs = res_tup[0]

                            if not recs: st.warning("Aucune donnée trouvée."); return
                            nodes, rels = set(), []
                            for r in recs:
                                if r['n']: nodes.add(r['n'])
                                if r['m']: nodes.add(r['m'])
                                if r['r']: rels.append(r['r'])

                            cfg = {'physics': physics}
                            if physics: cfg['physics'] = {'solver':'barnesHut', 'barnesHut':{'gravitationalConstant':-8000, 'springLength':150, 'avoidOverlap':0.1}, 'stabilization':{'iterations':150}}

                            st.write(f"Affichage {len(nodes)} nœuds, {len(rels)} relations.")
                            display_optimized_graph(nodes=list(nodes), relationships=rels, layout_config=cfg, async_rendering=True)

                        # --- Gestion des Erreurs ---
                        except CypherSyntaxError as e: st.error(f"Erreur Syntaxe Cypher: {e}")
                        except Exception as e: st.error(f"Erreur récupération/affichage Neo4j: {e}"); st.exception(e)
        except NotImplementedError as e: st.warning(f"Fonctionnalité Neo4j non disponible: {e}")
        except Exception as e: st.error(f"Erreur connexion Neo4j: {e}")


# --- Page d'Analyse et Visualisation  ---
#fonction pour afficher la page d'analyse et de visualisation
def show_analysis_visualization_page():
    st.header("Analyse & Visualisation")
    viz_type = st.selectbox("Source de données pour Visualisation/Analyse", ["MongoDB", "Neo4j"])

    # ==================
    # MongoDB Analysis & Viz Page
    # ==================

    if viz_type == "MongoDB":
        st.subheader("Analyse des données MongoDB")
        try:
            with MongoDBConnection(MONGODB_URI) as client:
                default_db = "Projet"
                default_coll = "movies"
                database_name = st.text_input("Base de données", default_db, key="mongodb_viz_db")
                collection_name = st.text_input("Collection", default_coll, key="mongodb_viz_coll")

                if not database_name or not collection_name:
                    st.warning("Veuillez entrer une base et une collection.")
                    return

                db = get_database(client, database_name)
                collection = get_collection(db, collection_name)
                st.info(f"Analyse de `{database_name}.{collection_name}`")

                analysis_option = st.selectbox("Choisir une analyse/visualisation",
                                               [
                                                "Distribution du Metascore",
                                                "Distribution des Classifications (Rating)",
                                                "Nombre de Films par Année",
                                                "Top Genres",
                                                "Metascore vs. Nombre de Votes (Scatter)"
                                                ])

                # --- Distribution du Metascore ---
                if analysis_option == "Distribution du Metascore":
                    with st.spinner("Chargement des données Metascore..."):
                        data = list(collection.find(
                            {"Metascore": {"$exists": True, "$ne": None, "$ne": ""}},
                            {"_id": 0, "Metascore": 1}
                        ))
                    if data:
                        df = pd.DataFrame(data)
                        df['Metascore'] = pd.to_numeric(df['Metascore'], errors='coerce')
                        df.dropna(subset=['Metascore'], inplace=True)
                        if not df.empty:
                            st.write(f"Distribution du Metascore ({len(df)} films avec score valide) :")
                            fig, ax = plt.subplots()
                            sns.histplot(df['Metascore'], kde=True, ax=ax, bins=20)
                            ax.set_title("Distribution du Metascore des Films")
                            ax.set_xlabel("Metascore")
                            ax.set_ylabel("Nombre de Films")
                            display_plot(fig)
                        else: st.warning("Aucune donnée Metascore numérique valide trouvée.")
                    else: st.warning("Aucune donnée trouvée pour 'Metascore'.")

                # --- Distribution des Classifications (Rating) ---
                elif analysis_option == "Distribution des Classifications (Rating)":
                     with st.spinner("Chargement des données de classification..."):
                         pipeline = [
                             {"$match": {"rating": {"$exists": True, "$ne": None, "$ne": ""}}},
                             {"$group": {"_id": "$rating", "count": {"$sum": 1}}},
                             {"$sort": {"count": -1}}
                         ]
                         data = list(collection.aggregate(pipeline))
                     if data:
                         df = pd.DataFrame(data)
                         df.rename(columns={"_id": "classification", "count": "nombre"}, inplace=True)
                         st.write(f"Distribution des Classifications ({len(df)} types trouvés) :")
                         fig, ax = plt.subplots(figsize=(8, max(5, len(df) * 0.5))) # Ajuster hauteur
                         sns.barplot(data=df, x='nombre', y='classification', ax=ax, palette="coolwarm")
                         
                         ax.set_title("Répartition des Films par Classification (Rating)")
                         ax.set_xlabel("Nombre de Films")
                         ax.set_ylabel("Classification")
                         plt.tight_layout()
                         display_plot(fig)
                     else: st.warning("Aucune donnée trouvée pour 'rating'.")

                # --- Nombre de Films par Année ---
                elif analysis_option == "Nombre de Films par Année":
                     with st.spinner("Chargement des données par année..."):
                         pipeline = [
                             # Conversion en nombre pour trier correctement
                             {"$addFields": { "year_num": {"$toInt": "$year"}}},
                             {"$match": {"year_num": {"$exists": True, "$ne": None}}},
                             {"$group": {"_id": "$year_num", "count": {"$sum": 1}}},
                             {"$sort": {"_id": 1}}
                         ]
                         data = list(collection.aggregate(pipeline))
                     if data:
                         df = pd.DataFrame(data)
                         df.rename(columns={"_id": "year"}, inplace=True)
                         current_year = pd.Timestamp.now().year
                         df = df[(df['year'] > 1900) & (df['year'] <= current_year)]
                         if not df.empty:
                             st.write(f"Nombre de films par année ({len(df)} années distinctes) :")
                             fig, ax = plt.subplots(figsize=(12, 6))
                             sns.lineplot(data=df, x='year', y='count', ax=ax, marker='o')
                             ax.set_title("Nombre de Films par Année")
                             ax.set_xlabel("Année")
                             ax.set_ylabel("Nombre de Films")
                             plt.xticks(rotation=45, ha='right')
                             plt.tight_layout()
                             display_plot(fig)
                         else: st.warning("Aucune donnée d'année valide (1901-présent) trouvée après conversion.")
                     else: st.warning("Aucune donnée trouvée pour 'year' ou conversion en nombre impossible.")

                # --- Top Genres ---
                elif analysis_option == "Top Genres":
                    with st.spinner("Analyse des genres..."):
                        data = list(collection.find(
                            {"genre": {"$exists": True, "$ne": None, "$ne": ""}},
                            {"_id": 0, "genre": 1}
                        ))
                    if data:
                        df = pd.DataFrame(data)
                        try:
                            
                            genre_series = df['genre'].str.split(',').explode().str.strip()
                            genre_counts = genre_series.value_counts().head(15)
                            if not genre_counts.empty:
                                top_genres_df = genre_counts.reset_index()
                                top_genres_df.columns = ['genre', 'count']
                                st.write(f"Top {len(top_genres_df)} Genres les plus fréquents :")
                                fig, ax = plt.subplots(figsize=(10, 7))
                                sns.barplot(data=top_genres_df, x='count', y='genre', ax=ax, palette="viridis")
                                ax.set_title("Top Genres de Films")
                                ax.set_xlabel("Nombre de Films")
                                ax.set_ylabel("Genre")
                                plt.tight_layout()
                                display_plot(fig)
                            else: st.warning("Aucun genre trouvé après traitement.")
                        except Exception as e:
                            st.error(f"Erreur lors du traitement des genres avec Pandas: {e}")
                            st.info("Vérifiez que le champ 'genre' contient bien des chaînes séparées par des virgules.")
                    else: st.warning("Aucune donnée trouvée pour 'genre'.")

                # --- Metascore ---
                elif analysis_option == "Metascore vs. Nombre de Votes (Scatter)":
                    with st.spinner("Chargement des données Metascore et Votes..."):
                         data = list(collection.find(
                            {
                                "Metascore": {"$exists": True, "$ne": None, "$ne": ""},
                                "Votes": {"$exists": True, "$ne": None} # Type vérifié après
                            },
                            {"_id": 0, "Metascore": 1, "Votes": 1, "title": 1}
                         ).limit(2000))
                    if data:
                        df = pd.DataFrame(data)
                        df['Metascore'] = pd.to_numeric(df['Metascore'], errors='coerce')
                        df['Votes'] = pd.to_numeric(df['Votes'], errors='coerce') # Convertir en nombre
                        df.dropna(subset=['Metascore', 'Votes'], inplace=True)
                        if not df.empty:
                            st.write(f"Relation entre Metascore et Nombre de Votes ({len(df)} films) :")
                            fig, ax = plt.subplots(figsize=(10, 6))
                            sns.scatterplot(data=df, x='Votes', y='Metascore', ax=ax, alpha=0.5)
                            ax.set_title("Metascore vs. Nombre de Votes")
                            ax.set_xlabel("Nombre de Votes")
                            ax.set_ylabel("Metascore")
                            if not df.empty and df['Votes'].max() / max(1, df['Votes'].min()) > 100: # Éviter division par zéro
                                ax.set_xscale('log')
                                ax.set_xlabel("Nombre de Votes (échelle log)")
                            plt.tight_layout()
                            display_plot(fig)
                        else: st.warning("Aucune donnée valide pour Metascore et Votes après nettoyage/conversion.")
                    else: st.warning("Aucune donnée trouvée avec 'Metascore' et 'Votes'.")

        except NotImplementedError:
             st.warning("Cette section nécessite que les fonctions de connexion MongoDB soient correctement implémentées et importées.")
        except Exception as e:
            st.error(f"Erreur lors de l'analyse MongoDB: {e}")
            st.exception(e)

    # ==================
    # Neo4j Graph Viz Page
    # ==================
    elif viz_type == "Neo4j":
        st.subheader("Visualisation du Graphe Neo4j")
        try:
            with Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) as driver:
                st.info("Connecté à Neo4j pour la visualisation.")

                col1, col2 = st.columns(2)
                with col1:
                    limit = st.number_input("Nombre max de nœuds à afficher", min_value=10, max_value=5000, value=150, key="viz_limit")
                with col2:
                    try:
                         with driver.session(database=neo4j_db) as session: # S'assurer que neo4j_db est défini
                             labels_result = session.run("CALL db.labels() YIELD label RETURN label")
                             available_labels = [record["label"] for record in labels_result]
                             default_selection = [lbl for lbl in ["Film", "Actor", "Director", "Genre"] if lbl in available_labels]
                    except NameError: # Si neo4j_db n'est pas défini globalement
                         neo4j_db = "neo4j" # Définir une valeur par défaut pour neo4j_db
                         with driver.session(database=neo4j_db) as session:
                              labels_result = session.run("CALL db.labels() YIELD label RETURN label")
                              available_labels = [record["label"] for record in labels_result]
                              default_selection = [lbl for lbl in ["Film", "Actor", "Director", "Genre"] if lbl in available_labels]

                    # Sélection des labels
                    except Exception as e:
                         st.warning(f"Impossible de récupérer les labels Neo4j: {e}. Utilisation de valeurs par défaut.")
                         available_labels = ["Film", "Actor", "Director", "Genre", "Person"]
                         default_selection = ["Film", "Actor"]
                    selected_labels = st.multiselect("Filtrer par Labels", available_labels, default=default_selection, key="viz_labels")

                # Activer la simulation physique
                physics_enabled = st.checkbox("Activer la simulation physique", value=True, key="viz_physics")

                if st.button("Afficher Graphe", key="viz_show_graph"):
                    with st.spinner("Construction du graphe Neo4j..."):
                        try:
                            label_filter_cypher = ""
                            params = {"limit": limit, "labels": None}
                            if selected_labels:
                                # Créer une condition WHERE pour les labels
                                label_filter_cypher = "WHERE size([lbl IN labels(n) WHERE lbl IN $labels]) > 0 AND ($labels IS NULL OR size([lbl IN labels(m) WHERE lbl IN $labels]) > 0)"
                                params["labels"] = selected_labels
                            else:
                                label_filter_cypher = "" 

                          # Requête Cypher pour récupérer les nœuds et relations
                            query = f"""
                            MATCH (n)
                            { "WHERE size([lbl IN labels(n) WHERE lbl IN $labels]) > 0" if selected_labels else "" }
                            WITH n LIMIT $limit
                            OPTIONAL MATCH (n)-[r]-(m)
                            { "WHERE $labels IS NULL OR size([lbl IN labels(m) WHERE lbl IN $labels]) > 0" if selected_labels else "" }
                            RETURN n, r, m
                            """

                            # Exécuter la requête
                            current_params = {"limit": limit}
                            if selected_labels:
                                current_params["labels"] = selected_labels


                            result_tuple = driver.execute_query(
                                query,
                                current_params, # Utiliser les bons paramètres
                                database_=neo4j_db
                             )
                            records = result_tuple[0]

                            # Vérifier si des enregistrements ont été trouvés
                            if not records:
                                st.warning("Aucune donnée trouvée pour les filtres sélectionnés.")
                                st.markdown("- Vérifiez que l'intégration a bien eu lieu.\n- Vérifiez les labels sélectionnés.\n- Augmentez la limite si besoin.")
                                return

                            nodes_dict = {} # Utiliser un dict pour éviter doublons et faciliter accès
                            relationships = []
                            rel_ids = set() # Pour éviter les relations dupliquées si le graphe est dense

                            for record in records:
                                n = record['n']
                                r = record['r']
                                m = record['m']

                                if n:
                                    nodes_dict[n.element_id] = n # Stocker par ID Neo4j interne
                                if m:
                                    nodes_dict[m.element_id] = m

                                # Ajouter la relation si elle existe et n'est pas déjà ajoutée
                                if r and r.element_id not in rel_ids:
                                    relationships.append(r)
                                    rel_ids.add(r.element_id)

                            nodes_list = list(nodes_dict.values()) # Récupérer la liste unique de nœuds

                            layout_config = {'physics': physics_enabled}
                            if physics_enabled:
                                layout_config['physics'] = {
                                    'solver': 'barnesHut',
                                    'barnesHut': { 'gravitationalConstant': -8000, 'springLength': 150, 'avoidOverlap': 0.1 },
                                    'stabilization': {'iterations': 150}
                                }
                            
                            st.write(f"Affichage de {len(nodes_list)} nœuds et {len(relationships)} relations.")
                            if not relationships:
                                st.warning("Aucune relation trouvée entre les nœuds affichés avec les filtres actuels.")

                            # Afficher le graphe
                            display_optimized_graph(
                                nodes=nodes_list,
                                relationships=relationships,
                                layout_config=layout_config,
                                async_rendering=True
                            )

                        except CypherSyntaxError as e:
                             st.error(f"Erreur de syntaxe Cypher (Visualisation): {e}")
                        except Exception as e:
                            st.error(f"Erreur lors de la récupération/affichage du graphe Neo4j: {e}")
                            st.exception(e)

        except NotImplementedError:
            st.warning("Cette section nécessite que la fonction de connexion Neo4j (`Neo4jConnection`) soit correctement implémentée et importée.")
        except Exception as e:
            st.error(f"Erreur de connexion à Neo4j pour la visualisation: {e}")


# --- Classes d'Erreur de Connexion Placeholder ---
class MongoDBConnectionError(Exception): pass
class Neo4jConnectionError(Exception): pass


# --- Point d'Entrée ---
if __name__ == "__main__":
    # Définir le style Seaborn
    sns.set_theme(style="whitegrid")
    main()