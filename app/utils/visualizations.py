"""
Module de visualisation des données pour MongoDB et Neo4j
"""
from typing import List, Dict, Any
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from neo4j import Session
from pyvis.network import Network
import streamlit as st
import tempfile
import networkx as nx  
from pyvis.network import Network

def create_mongodb_bar_chart(data: List[Dict[str, Any]], 
                           x_field: str, 
                           y_field: str,
                           title: str = "") -> go.Figure:
    """
    Crée un graphique en barres à partir des données MongoDB.
    
    Args:
        data (List[Dict[str, Any]]): Données MongoDB
        x_field (str): Champ pour l'axe X
        y_field (str): Champ pour l'axe Y
        title (str): Titre du graphique
        
    Returns:
        go.Figure: Figure Plotly
    """
    df = pd.DataFrame(data)
    fig = px.bar(df, x=x_field, y=y_field, title=title)
    return fig

def create_mongodb_pie_chart(data: List[Dict[str, Any]],
                           names_field: str,
                           values_field: str,
                           title: str = "") -> go.Figure:
    """
    Crée un graphique circulaire à partir des données MongoDB.
    
    Args:
        data (List[Dict[str, Any]]): Données MongoDB
        names_field (str): Champ pour les noms des sections
        values_field (str): Champ pour les valeurs
        title (str): Titre du graphique
        
    Returns:
        go.Figure: Figure Plotly
    """
    df = pd.DataFrame(data)
    fig = px.pie(df, names=names_field, values=values_field, title=title)
    return fig

def create_mongodb_line_chart(data: List[Dict[str, Any]],
                            x_field: str,
                            y_field: str,
                            title: str = "") -> go.Figure:
    """
    Crée un graphique en ligne à partir des données MongoDB.
    
    Args:
        data (List[Dict[str, Any]]): Données MongoDB
        x_field (str): Champ pour l'axe X
        y_field (str): Champ pour l'axe Y
        title (str): Titre du graphique
        
    Returns:
        go.Figure: Figure Plotly
    """
    df = pd.DataFrame(data)
    fig = px.line(df, x=x_field, y=y_field, title=title)
    return fig

def create_neo4j_graph_visualization(session: Session, 
                                   limit: int = 100,
                                   height: str = "600px",
                                   width: str = "100%") -> str:
    """
    Crée une visualisation interactive du graphe Neo4j.
    
    Args:
        session (Session): Session Neo4j
        limit (int): Nombre maximum de nœuds à afficher
        height (str): Hauteur du graphe
        width (str): Largeur du graphe
        
    Returns:
        str: Chemin du fichier HTML temporaire contenant la visualisation
    """
    # Création du réseau
    net = Network(height=height, width=width, notebook=True)
    

    
    # Récupération des nœuds
    query = f"""
    MATCH (n)
    WITH n LIMIT {limit}
    RETURN id(n) as id, labels(n) as labels, properties(n) as properties
    """
    nodes_result = session.run(query)
    
    # Ajout des nœuds au réseau
    for record in nodes_result:
        node_id = record["id"]
        labels = record["labels"]
        properties = record["properties"]
        
        # Création du titre avec les propriétés
        title = "<br>".join([f"{k}: {v}" for k, v in properties.items()])
        
        # Utilisation du premier label comme groupe pour la couleur
        group = labels[0] if labels else "No Label"
        
        # Utilisation de la première propriété comme label, sinon l'ID
        label = next(iter(properties.values()), str(node_id))
        
        net.add_node(node_id, label=str(label), title=title, group=group)
    
    # Récupération des relations
    query = f"""
    MATCH (n)-[r]->(m)
    WHERE id(n) IN range(0, {limit}) AND id(m) IN range(0, {limit})
    RETURN id(n) as source, id(m) as target, type(r) as type, properties(r) as properties
    """
    edges_result = session.run(query)
    
    # Ajout des relations au réseau
    for record in edges_result:
        source = record["source"]
        target = record["target"]
        rel_type = record["type"]
        properties = record["properties"]
        
        # Création du titre avec les propriétés
        title = "<br>".join([f"{k}: {v}" for k, v in properties.items()])
        
        net.add_edge(source, target, title=title, label=rel_type)
    
    # Configuration du graphe
    net.toggle_physics(True)
    net.show_buttons(filter_=['physics'])
    
    # Création d'un fichier temporaire pour la visualisation
    with tempfile.NamedTemporaryFile(delete=False, suffix='.html') as tmp_file:
        net.save_graph(tmp_file.name)
        return tmp_file.name

#affichage du graphe Neo4j
def display_neo4j_graph(driver, limit: int = 100):
    with driver.session() as session:  
        html_file = create_neo4j_graph_visualization(session, limit)
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
        st.components.v1.html(html_content, height=600)

def display_optimized_graph(nodes, relationships, layout_config=None, async_rendering=False):
    """Version améliorée de display_neo4j_graph avec gestion des gros graphes"""
    
    # Conversion des objets Neo4j vers des formats NetworkX compatibles
    G = nx.Graph()
    
    # Ajout des nœuds avec leurs propriétés
    for node in nodes:
        props = dict(node.items())
        G.add_node(node.id, label=props.get('name') or props.get('title'), **props)
    
    # Ajout des relations
    for rel in relationships:
        G.add_edge(rel.start_node.id, rel.end_node.id, label=rel.type)
    
    # Configuration PyVis
    net = Network(height="800px", 
                width="100%", 
                bgcolor="#222222", 
                font_color="white",
                directed=True,
                layout=layout_config)
    
    # Transfert du graphe NetworkX vers PyVis
    net.from_nx(G)
    
    # Optimisations pour les gros datasets
    net.set_options("""
    {
        "nodes": {
            "scaling": {
                "min": 10,
                "max": 30
            }
        },
        "edges": {
            "smooth": {
                "type": "continuous"
            },
            "arrowStrikethrough": false
        },
        "physics": {
            "stabilization": {
                "enabled": true,
                "iterations": 100
            }
        }
    }
    """)
    
    # Génération du HTML
    with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as tmpfile:
        net.save_graph(tmpfile.name)
        with open(tmpfile.name, "r", encoding="utf-8") as f:
            html = f.read()
    
    # Affichage Streamlit
    st.components.v1.html(html, height=800, scrolling=True)
