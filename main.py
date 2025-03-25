import json
import streamlit as st
from config import MONGODB_URI, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from app.database.mongodb import MongoDBConnection, get_database, get_collection
from app.database.neo4j import Neo4jConnection
from app.queries.mongodb_queries import (
    insert_document, find_documents, update_documents,
    delete_documents, aggregate_documents
)
from app.queries.neo4j_queries import (
    create_node, find_nodes, create_relationship,
    find_shortest_path, execute_cypher_query, analyze_graph
)
from app.utils.visualizations import (
    create_mongodb_bar_chart, create_mongodb_pie_chart,
    create_mongodb_line_chart, display_neo4j_graph
)

#fonction principale pour lancer l'application
def main():
    st.set_page_config(page_title="Application NoSQL", layout="wide")
    st.title("NoSQL Application - MongoDB & Neo4j")
    
    #création des onglets pour naviguer entre les différentes pages
    tab1, tab2, tab3, tab4 = st.tabs(["Accueil", "MongoDB", "Neo4j", "Visualisation"])
    
    #affichage de la page d'accueil
    with tab1:
        show_home()
    
    #affichage de la page de gestion MongoDB
    with tab2:
        show_mongodb_page()
    
    #affichage de la page de gestion Neo4j
    with tab3:
        show_neo4j_page()
    
    #affichage de la page de visualisation
    with tab4:
        show_visualization_page()

#affichage de la page d'accueil
def show_home():
    st.header("Bienvenue dans l'application NoSQL")
    st.info("Cette application permet d'interagir avec MongoDB et Neo4j via cette interface.")

#affichage de la page de gestion MongoDB
def show_mongodb_page():
    st.header("📄 Gestion MongoDB")
    
    #connexion à MongoDB
    with MongoDBConnection(MONGODB_URI) as client:
        st.success("✅ Connexion MongoDB réussie")
        database_name = st.text_input("Base de données", "sample_db", key="mongodb_db_input")
        collection_name = st.text_input("Collection", "sample_collection", key="mongodb_collection_input")
        db = get_database(client, database_name)
        collection = get_collection(db, collection_name)

        #recherche de documents
        with st.expander("🔍 Rechercher des documents"):
            query_str = st.text_area("Critères (JSON)", "{}", key="search_query")
            if st.button("Rechercher", key="search_button"):
                try:
                    results = find_documents(collection, json.loads(query_str))
                    st.json(results)
                except json.JSONDecodeError:
                    st.error("Format JSON invalide")

        #insertion de documents
        with st.expander("➕ Insérer un document"):
            document_str = st.text_area("Document (JSON)", "{}", key="insert_document")
            if st.button("Insérer", key="insert_button"):
                try:
                    result = insert_document(collection, json.loads(document_str))
                    st.toast(f"✅ Document inséré avec ID: {result}")
                except json.JSONDecodeError:
                    st.error("Format JSON invalide")

        #mise à jour de documents
        with st.expander("✏️ Mettre à jour"):
            query_str = st.text_area("Critères (JSON)", "{}", key="update_query")
            update_str = st.text_area("Mise à jour (JSON)", "{}", key="update_data")
            if st.button("Mettre à jour", key="update_button"):
                try:
                    count = update_documents(collection, json.loads(query_str), json.loads(update_str))
                    st.toast(f"✅ {count} document(s) mis à jour")
                except json.JSONDecodeError:
                    st.error("Format JSON invalide")
        
        #suppression de documents
        with st.expander("🗑️ Supprimer"):
            query_str = st.text_area("Critères (JSON)", "{}", key="delete_query")
            if st.button("Supprimer", key="delete_button"):
                try:
                    count = delete_documents(collection, json.loads(query_str))
                    st.toast(f"✅ {count} document(s) supprimé(s)")
                except json.JSONDecodeError:
                    st.error("Format JSON invalide")

#affichage de la page de gestion Neo4j
def show_neo4j_page():
    st.header("🔗 Gestion Neo4j")
    
    #connexion à Neo4j
    with Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) as driver:
        st.success("✅ Connexion Neo4j réussie")
        
        #création d'un nœud
        with st.expander("➕ Créer un nœud"):
            label = st.text_input("Label", key="neo4j_label_input")
            properties_str = st.text_area("Propriétés (JSON)", "{}", key="create_node")
            if st.button("Créer", key="create_node_button"):
                try:
                    result = create_node(driver, label, json.loads(properties_str))
                    st.toast("✅ Nœud créé avec succès")
                except json.JSONDecodeError:
                    st.error("Format JSON invalide")
        
        #recherche de nœuds
        with st.expander("🔍 Rechercher des nœuds"):
            label = st.text_input("Label (optionnel)", key="search_nodes_input")
            if st.button("Rechercher", key="search_nodes_button"):
                results = find_nodes(driver, label if label else None)
                st.json(results)
        
        #création d'une relation
        with st.expander("🔗 Créer une relation"):
            start_id = st.number_input("ID de départ", min_value=0, step=1)
            end_id = st.number_input("ID d'arrivée", min_value=0, step=1)
            rel_type = st.text_input("Type de relation", key="create_relationship_input")
            if st.button("Créer relation", key="create_relationship_button"):
                if create_relationship(driver, start_id, end_id, rel_type):
                    st.toast("✅ Relation créée avec succès")

#affichage de la page de visualisation          
def show_visualization_page():
    st.header("📊 Visualisation des données")
    
    viz_type = st.selectbox("Type de visualisation", ["MongoDB", "Neo4j"])
    
    #affichage de la page de visualisation MongoDB
    if viz_type == "MongoDB":
        with MongoDBConnection(MONGODB_URI) as client:
            database_name = st.text_input("Base de données", "sample_db", key="visualization_db_input")
            collection_name = st.text_input("Collection", "sample_collection", key="visualization_collection_input")
            db = get_database(client, database_name)
            collection = get_collection(db, collection_name)
            chart_type = st.selectbox("Type de graphique", ["Barres", "Circulaire", "Ligne"])
            
            if chart_type in ["Barres", "Ligne"]:
                x_field = st.text_input("Axe X", key="x_axis_input")
                y_field = st.text_input("Axe Y", key="y_axis_input")
                if st.button("Générer", key="generate_chart_button"):
                    data = find_documents(collection)
                    fig = create_mongodb_bar_chart(data, x_field, y_field) if chart_type == "Barres" else create_mongodb_line_chart(data, x_field, y_field)
                    st.plotly_chart(fig)
            else:
                names_field = st.text_input("Noms", key="names_input")
                values_field = st.text_input("Valeurs", key="values_input")
                if st.button("Générer", key="generate_pie_chart_button"):
                    data = find_documents(collection)
                    fig = create_mongodb_pie_chart(data, names_field, values_field)
                    st.plotly_chart(fig)
    else:
        #affichage du graphe Neo4j
        with Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) as driver:
            limit = st.slider("Nombre max de nœuds", 10, 500, 100)
            if st.button("Afficher graphe", key="display_graph_button"):
                display_neo4j_graph(driver, limit)

if __name__ == "__main__":
    main()