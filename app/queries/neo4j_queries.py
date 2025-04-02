"""
Module contenant les opérations pour Neo4j
"""
from typing import List, Dict, Any, Optional
from neo4j import Session
from typing import List, Dict, Any, Optional
from neo4j import Session
import pandas as pd

# Fonction pour créer un nœud
def create_node(session: Session, label: str, properties: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Crée un nœud dans le graphe Neo4j.
    
    Args:
        session (Session): Session Neo4j
        label (str): Label du nœud
        properties (Dict[str, Any]): Propriétés du nœud
        
    Returns:
        Optional[Dict[str, Any]]: Propriétés du nœud créé si succès, None sinon
    """
    query = f"CREATE (n:{label} $props) RETURN n"
    try:
        result = session.run(query, props=properties)
        record = result.single()
        return dict(record["n"]) if record else None
    except Exception as e:
        print(f"Erreur lors de la création du nœud: {str(e)}")
        return None

# Fonction pour trouver un nœud par ID
def find_nodes(session: Session, label: Optional[str] = None, 
               properties: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Recherche des nœuds dans le graphe.
    
    Args:
        session (Session): Session Neo4j
        label (str, optional): Label des nœuds à rechercher
        properties (Dict[str, Any], optional): Propriétés à matcher
        
    Returns:
        List[Dict[str, Any]]: Liste des nœuds trouvés
    """
    if label:
        query = f"MATCH (n:{label})"
    else:
        query = "MATCH (n)"
        
    if properties:
        conditions = " AND ".join(f"n.{k} = ${k}" for k in properties.keys())
        query += f" WHERE {conditions}"
    
    query += " RETURN n"
    
    try:
        result = session.run(query, **properties if properties else {})
        return [dict(record["n"]) for record in result]
    except Exception as e:
        print(f"Erreur lors de la recherche des nœuds: {str(e)}")
        return []

# Fonction pour créer une relation entre deux nœuds
def create_relationship(session, start_id, end_id, rel_type, props=None):
    """Crée une relation entre deux nœuds de manière sécurisée"""
    # Validation du type de relation
    if not re.match(r'^[A-Z_]{1,50}$', rel_type):
        raise ValueError("Type de relation invalide (doit être en majuscules, max 50 caractères)")
    
    # Construction dynamique sécurisée de la requête
    query = f"""
    MATCH (start), (end)
    WHERE ID(start) = $start_id AND ID(end) = $end_id
    CREATE (start)-[r:{rel_type}]->(end)
    SET r += $props
    RETURN COUNT(r) AS count
    """
    
    try:
        result = session.run(query, 
                            start_id=start_id, 
                            end_id=end_id,
                            props=props or {})
        return result.single()["count"] > 0
    except Exception as e:
        raise RuntimeError(f"Erreur Neo4j: {str(e)}") from e

# Fonction pour supprimer une relation entre deux nœuds
def find_shortest_path(session: Session, start_node_id: int, end_node_id: int,
                      rel_type: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Trouve le plus court chemin entre deux nœuds.
    
    Args:
        session (Session): Session Neo4j
        start_node_id (int): ID du nœud de départ
        end_node_id (int): ID du nœud d'arrivée
        rel_type (str, optional): Type de relation à considérer
        
    Returns:
        List[Dict[str, Any]]: Liste des nœuds formant le chemin
    """
    if rel_type:
        query = f"""
        MATCH path = shortestPath((start)-[:{rel_type}*]-(end))
        WHERE ID(start) = $start_id AND ID(end) = $end_id
        RETURN nodes(path) as nodes
        """
    else:
        query = """
        MATCH path = shortestPath((start)-[*]-(end))
        WHERE ID(start) = $start_id AND ID(end) = $end_id
        RETURN nodes(path) as nodes
        """
    
    try:
        result = session.run(query, start_id=start_node_id, end_id=end_node_id)
        record = result.single()
        return [dict(node) for node in record["nodes"]] if record else []
    except Exception as e:
        print(f"Erreur lors de la recherche du plus court chemin: {str(e)}")
        return []
    
# Fonction pour exécuter une requête Cypher personnalisée
def execute_cypher_query(session: Session, query: str, 
                        parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Exécute une requête Cypher personnalisée.
    
    Args:
        session (Session): Session Neo4j
        query (str): Requête Cypher
        parameters (Dict[str, Any], optional): Paramètres de la requête
        
    Returns:
        List[Dict[str, Any]]: Résultats de la requête
    """
    try:
        result = session.run(query, parameters or {})
        return [dict(record) for record in result]
    except Exception as e:
        print(f"Erreur lors de l'exécution de la requête: {str(e)}")
        return []

# Fonction pour obtenir le plus court chemin entre deux acteurs
def get_shortest_path_between_actors(session: Session, actor1_name: str, actor2_name: str) -> List[Dict[str, Any]]:
    query = """
    MATCH (a1:Actor {name: $actor1_name}), (a2:Actor {name: $actor2_name})
    MATCH path = shortestPath((a1)-[:ACTED_IN*..10]-(a2))
    RETURN [node IN nodes(path) | {type: labels(node)[0], properties: properties(node)}] AS path_nodes, 
           [rel IN relationships(path) | type(rel)] AS path_relationships
    """
    try:
        result = session.run(query, actor1_name=actor1_name, actor2_name=actor2_name)
        record = result.single()
        if record:
            return {"nodes": record["path_nodes"], "relationships": record["path_relationships"]}
        return []
    except Exception as e:
        print(f"Erreur chemin entre acteurs: {e}")
        return []

# Fonction pour analyser le graphe
def analyze_graph(session: Session) -> Dict[str, Any]:
    """
    Effectue une analyse basique du graphe.
    
    Args:
        session (Session): Session Neo4j
        
    Returns:
        Dict[str, Any]: Statistiques du graphe
    """
    stats = {}
    
    try:
        # Nombre total de nœuds
        result = session.run("MATCH (n) RETURN count(n) as count")
        stats["total_nodes"] = result.single()["count"]
        
        # Nombre total de relations
        result = session.run("MATCH ()-[r]->() RETURN count(r) as count")
        stats["total_relationships"] = result.single()["count"]
        
        # Distribution des labels
        result = session.run("""
        MATCH (n)
        RETURN distinct labels(n) as label, count(*) as count
        ORDER BY count DESC
        """)
        stats["label_distribution"] = {
            str(record["label"]): record["count"] 
            for record in result
        }
        
        # Distribution des types de relations
        result = session.run("""
        MATCH ()-[r]->()
        RETURN distinct type(r) as type, count(*) as count
        ORDER BY count DESC
        """)
        stats["relationship_distribution"] = {
            record["type"]: record["count"] 
            for record in result
        }
        
        return stats
    except Exception as e:
        print(f"Erreur lors de l'analyse du graphe: {str(e)}")
        return {} 