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
   
    parameters = parameters or {}
    result = session.run(query, parameters)
    return [record for record in result]


