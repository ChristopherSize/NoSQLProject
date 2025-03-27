import json
import streamlit as st
from config import MONGODB_URI, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from app.database.mongodb import MongoDBConnection, get_database, get_collection
from app.database.neo4j import Neo4jConnection
from app.queries.mongodb_queries import find_documents, insert_document, update_documents, delete_documents
from app.queries.neo4j_queries import create_node, find_nodes, create_relationship
 

# Fonction principale pour lancer l'application
def main():
    st.set_page_config(page_title="Application NoSQL", layout="wide")
    st.title("NoSQL Application - MongoDB & Neo4j")
    
    # Création des onglets
    tab1, tab2, tab3, tab4 = st.tabs(["Accueil", "MongoDB", "Neo4j", "Visualisation"])
    
    with tab1:
        show_home()
    
    with tab2:
        show_mongodb_page()
    
    with tab3:
        show_neo4j_page()
    
    with tab4:
        show_visualization_page()

# Page d'accueil
def show_home():
    st.header("Bienvenue dans l'application NoSQL")
    st.info("Cette application permet d'interagir avec MongoDB et Neo4j via cette interface.")

# Page de gestion MongoDB
def show_mongodb_page():
    st.header("Gestion MongoDB")
    
    # Connexion à MongoDB
    with MongoDBConnection(MONGODB_URI) as client:
        st.success("Connexion MongoDB réussie")
        database_name = st.text_input("Base de données", "sample_db", key="mongodb_db_input")
        collection_name = st.text_input("Collection", "sample_collection", key="mongodb_collection_input")
        db = get_database(client, database_name)
        collection = get_collection(db, collection_name)

        # Section pour rechercher, insérer, mettre à jour et supprimer des documents
        
        with st.expander("Rechercher des documents"):
            query_str = st.text_area("Critères (JSON)", "{}", key="search_query")
            if st.button("Rechercher", key="search_button"):
                try:
                    results = find_documents(collection, json.loads(query_str))
                    st.json(results)
                except json.JSONDecodeError:
                    st.error("Format JSON invalide")

        # Section pour insérer des documents
        with st.expander("Insérer un document"):
            document_str = st.text_area("Document (JSON)", "{}", key="insert_document")
            if st.button("Insérer", key="insert_button"):
                try:
                    result = insert_document(collection, json.loads(document_str))
                    st.toast(f"Document inséré avec ID: {result}")
                except json.JSONDecodeError:
                    st.error("Format JSON invalide")

        # Section pour mettre à jour des documents
        with st.expander("Mettre à jour"):
            query_str = st.text_area("Critères (JSON)", "{}", key="update_query")
            update_str = st.text_area("Mise à jour (JSON)", "{}", key="update_data")
            if st.button("Mettre à jour", key="update_button"):
                try:
                    count = update_documents(collection, json.loads(query_str), json.loads(update_str))
                    st.toast(f"{count} document(s) mis à jour")
                except json.JSONDecodeError:
                    st.error("Format JSON invalide")
        
        # Section pour supprimer des documents
        with st.expander("Supprimer"):
            query_str = st.text_area("Critères (JSON)", "{}", key="delete_query")
            if st.button("Supprimer", key="delete_button"):
                try:
                    count = delete_documents(collection, json.loads(query_str))
                    st.toast(f"{count} document(s) supprimé(s)")
                except json.JSONDecodeError:
                    st.error("Format JSON invalide")

# Page de gestion Neo4j avec intégration MongoDB
def show_neo4j_page():
    st.header("Gestion Neo4j")
    
    # Connexion à Neo4j
    with Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) as driver:
        st.success("Connexion Neo4j réussie")
        
        with st.expander("Créer un nœud"):
            label = st.text_input("Label", key="neo4j_label_input")
            properties_str = st.text_area("Propriétés (JSON)", "{}", key="create_node")
            if st.button("Créer", key="create_node_button"):
                try:
                    with driver.session() as session:
                        result = create_node(session, label, json.loads(properties_str))
                        st.toast("Nœud créé avec succès")
                except json.JSONDecodeError:
                    st.error("Format JSON invalide")
        
        # Section pour rechercher des nœuds
        with st.expander("Rechercher des nœuds"):
            label = st.text_input("Label (optionnel)", key="search_nodes_input")
            if st.button("Rechercher", key="search_nodes_button"):
                with driver.session() as session:
                    results = find_nodes(session, label if label else None)
                    st.json(results)
    
        # Section pour intégrer les données MongoDB dans Neo4j
        with st.expander("Intégrer les données MongoDB dans Neo4j"):
            mongo_db_name = st.text_input("Base MongoDB", "sample_db", key="mongo_db_input_neo4j")
            mongo_collection_name = st.text_input("Collection MongoDB", "Your collection", key="mongo_collection_input_neo4j")
            if st.button("Lancer l'intégration", key="integrate_button"):
                try:
                    integrate_mongodb_to_neo4j(driver, mongo_db_name, mongo_collection_name)
                    st.success("Intégration terminée avec succès")
                except Exception as e:
                    st.error(f"Erreur lors de l'intégration: {str(e)}")

# Fonction pour intégrer les données de MongoDB dans Neo4j
def integrate_mongodb_to_neo4j(driver,  mongo_db_name, mongo_collection_name):
    """Intègre les données de MongoDB dans Neo4j avec nœuds et relations."""
    
    #Connexion à MongoDB
    with MongoDBConnection(MONGODB_URI) as mongo_client:
        db = get_database(mongo_client, mongo_db_name)
        collection = get_collection(db, mongo_collection_name)

        #Extraire les données des films
        films_data = list(find_documents(collection, {}, {"_id": 1 ,"title": 1, "year": 1, "Votes": 1, "Revenue": 1, "rating": 1, "Director": 1, "Actors": 1, "genre": 1}))
        directors = collection.distinct("Director")
        if not films_data:
            raise ValueError("Aucune donnée trouvée dans MongoDB")


        #Connexion à Neo4j et création des nœuds/relation
        with driver.session() as session:
            with session.begin_transaction() as tx:
                try:
                    #Créer des nœuds Film
                    for film in films_data:
                        if "title" not in film or "year" not in film:
                            st.warning(f"Données incomplètes pour le film: {film}")
                            continue
                        properties = {
                            "id": str(film["_id"]),
                            "title": film.get("title"),
                            "director": film.get("Director"),
                            "year": film.get("year"),
                            "rating": film.get("rating"),
                            "votes": film.get("Votes"),
                            "Revenue": film.get("Revenue (Millions)"),
                        }
                        create_node(tx, "Film", properties)

                    # Valider la transaction MongoDB -> Neo4j
                        tx.commit()
                        st.toast(f"{len(films_data)} films intégrés avec succès")
                except Exception as commit_error:
                        st.error(f"Échec du commit : {str(commit_error)}")
                        tx.rollback()

# Page de visualisation
def show_visualization_page():
    st.header("📊 Visualisation des données")
    viz_type = st.selectbox("Type de visualisation", ["MongoDB", "Neo4j"])
    
    
if __name__ == "__main__":
    main()