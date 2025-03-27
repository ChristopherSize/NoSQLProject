from typing import Optional
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

class MongoDBConnection:

    #fonction pour initialiser la connexion à MongoDB
    def __init__(self, uri: str):
        """
        Initialise la connexion à MongoDB.
        """
        self.uri = uri
        self._client: Optional[MongoClient] = None

    #fonction pour établir la connexion lors de l'entrée dans le contexte
    def __enter__(self) -> MongoClient:
        try:
            self._client = MongoClient(self.uri)
            return self._client
        except Exception as e:
            print(f"Erreur lors de la connexion à MongoDB: {str(e)}")
            raise

    #fonction pour fermer la connexion lors de la sortie du contexte
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._client is not None:
            self._client.close()

#fonction pour récupérer une base de données MongoDB
def get_database(client: MongoClient, database_name: str) -> Database:
   
    return client[database_name]

#fonction pour récupérer une collection MongoDB
def get_collection(database: Database, collection_name: str) -> Collection:
    
    return database[collection_name] 
