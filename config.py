import os
from dotenv import load_dotenv

# Chargement des variables d'environnement
load_dotenv()

# Configuration MongoDB
MONGODB_URI = os.getenv('MONGODB_URI')

# Configuration Neo4j
NEO4J_URI = os.getenv('NEO4J_URI')
NEO4J_USER = os.getenv('NEO4J_USER')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')

# Vérification de la configuration
if not all([MONGODB_URI, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD]):
    raise ValueError("Veuillez configurer toutes les variables d'environnement nécessaires dans le fichier .env") 