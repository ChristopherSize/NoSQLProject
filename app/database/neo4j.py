from typing import Optional
from neo4j import GraphDatabase, Driver, Session

class Neo4jConnection:
    
    #fonction pour initialiser la connexion à Neo4j
    def __init__(self, uri: str, user: str, password: str):
      
        self.uri = uri
        self.user = user
        self.password = password
        self._driver: Optional[Driver] = None
        
    #fonction pour établir la connexion lors de l'entrée dans le contexte
    def __enter__(self) -> Driver:
        self._driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        return self._driver
        
    #fonction pour fermer la connexion lors de la sortie du contexte
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._driver is not None:
            self._driver.close()

#fonction pour exécuter une requête Cypher
def execute_query(session: Session, query: str, parameters: dict = None) -> list:
    """
    Exécute une requête Cypher.
    
    Args:
        session (Session): Session Neo4j
        query (str): Requête Cypher
        parameters (dict, optional): Paramètres de la requête
        
    Returns:
        list: Résultats de la requête
    """
    parameters = parameters or {}
    result = session.run(query, parameters)
    return [record for record in result]

#fonction pour créer un noeud
def create_node(session: Session, label: str, properties: dict) -> dict:
    
    query = f"CREATE (n:{label} $props) RETURN n"
    result = execute_query(session, query, {"props": properties})
    return result[0]["n"] if result else None

#fonction pour créer une relation
def create_relationship(session: Session, start_node_id: int, end_node_id: int, 
                       relationship_type: str, properties: dict = None) -> dict:
    
    properties = properties or {}
    query = f"""
    MATCH (start), (end)
    WHERE ID(start) = $start_id AND ID(end) = $end_id
    CREATE (start)-[r:{relationship_type} $props]->(end)
    RETURN r
    """
    params = {
        "start_id": start_node_id,
        "end_id": end_node_id,
        "props": properties
    }
    result = execute_query(session, query, params)
    return result[0]["r"] if result else None 