# Application NoSQL - MongoDB et Neo4j

Cette application est une interface utilisateur web construite avec Streamlit permettant d'interagir avec deux bases de données NoSQL : MongoDB (base de données orientée document) et Neo4j (base de données orientée graphe). Elle offre des fonctionnalités CRUD (Create, Read, Update, Delete) complètes ainsi que des capacités de visualisation avancées.

## Prérequis

- Python 3.8 ou supérieur
- Compte MongoDB Atlas (pour la base de données MongoDB)
- Compte Neo4j AuraDB (pour la base de données Neo4j)
- pip (gestionnaire de paquets Python)

## Installation

1. Cloner le dépôt :
```bash
git clone [URL_DU_REPO]
cd [NOM_DU_DOSSIER]
```

2. Créer un environnement virtuel Python :
```bash
# Windows
python -m venv venv
.\venv\Scripts\activate

# macOS/Linux
python3 -m venv venv
source venv/bin/activate
```

3. Installer les dépendances :
```bash
pip install -r requirements.txt
```

4. Configuration des variables d'environnement :
   - Créer un fichier `.env` à la racine du projet
   - Ajouter les informations de connexion suivantes :
```env
# MongoDB Configuration
MONGODB_URI=votre_uri_mongodb

# Neo4j Configuration
NEO4J_URI=votre_uri_neo4j
NEO4J_USER=votre_utilisateur_neo4j
NEO4J_PASSWORD=votre_mot_de_passe_neo4j
```

## Lancement de l'application

```bash
streamlit run main.py
```

L'application sera accessible à l'adresse : http://localhost:8501



## Fonctionnalités principales

### MongoDB
1. **Opérations CRUD** :
   - Création de documents
   - Lecture avec filtres et projections
   - Mise à jour de documents
   - Suppression de documents
2. **Agrégation** : Exécution de pipelines d'agrégation
3. **Visualisation** :
   - Graphiques en barres
   - Graphiques circulaires
   - Graphiques en ligne

### Neo4j
1. **Gestion des nœuds** :
   - Création de nœuds avec labels et propriétés
   - Recherche de nœuds
2. **Gestion des relations** :
   - Création de relations typées avec propriétés
   - Recherche de relations
3. **Requêtes avancées** :
   - Exécution de requêtes Cypher personnalisées
   - Recherche de plus courts chemins
4. **Visualisation** :
   - Visualisation interactive du graphe
   - Statistiques du graphe (nombre de nœuds, relations, etc.)

## Guide d'utilisation

### Navigation
L'application comporte quatre sections principales :
1. **Accueil** : Page d'accueil et présentation
2. **MongoDB** : Interface pour MongoDB
3. **Neo4j** : Interface pour Neo4j
4. **Visualisation** : Outils de visualisation des données




