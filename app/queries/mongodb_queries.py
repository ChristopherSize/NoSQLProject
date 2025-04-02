"""
Module contenant les opérations CRUD pour MongoDB
"""
from typing import List, Dict, Any, Optional
from pymongo.collection import Collection
from pymongo.results import InsertOneResult, UpdateResult, DeleteResult
import logging

# Fonction pour insérer un document
def insert_document(collection: Collection, document: Dict[str, Any]) -> Optional[str]:
    """
    Insère un document dans une collection MongoDB.
    
    Args:
        collection (Collection): Collection MongoDB
        document (Dict[str, Any]): Document à insérer
        
    Returns:
        Optional[str]: ID du document inséré si succès, None sinon
    """
    try:
        result: InsertOneResult = collection.insert_one(document)
        return str(result.inserted_id)
    except Exception as e:
        print(f"Erreur lors de l'insertion: {str(e)}")
        return None

# Fonction pour trouver des documents
def find_documents(collection: Collection, query: Dict[str, Any] = None, 
                  projection: Dict[str, Any] = None, limit: int = None) -> List[Dict[str, Any]]:
    """
    Recherche des documents dans une collection MongoDB.
    
    Args:
        collection (Collection): Collection MongoDB
        query (Dict[str, Any], optional): Critères de recherche
        projection (Dict[str, Any], optional): Champs à retourner
        limit (int, optional): Nombre maximum de documents à retourner
        
    Returns:
        List[Dict[str, Any]]: Liste des documents trouvés
    """
    try:
        cursor = collection.find(query or {}, projection or {})
        if limit is not None:
            cursor = cursor.limit(limit)
        return list(cursor)
    except Exception as e:
        print(f"Erreur lors de la recherche: {str(e)}")
        return []


# Fonction pour mettre à jour des documents
def update_documents(collection: Collection, query: Dict[str, Any],
                    update_operation: Dict[str, Any], multi: bool = False) -> int:
    """
    Met à jour un ou plusieurs documents dans une collection MongoDB.

    Args:
        collection (Collection): La collection MongoDB.
        query (Dict[str, Any]): Critères de recherche.
        update_operation (Dict[str, Any]): Opération de mise à jour. 
            Si aucun opérateur MongoDB ($set/$inc/etc) n'est détecté,
            les champs sont automatiquement enveloppés dans un $set.
        multi (bool): True = update_many, False = update_one.

    Returns:
        int: Nombre de documents modifiés
    """
    try:
        # Détection auto des opérateurs de mise à jour
        if not any(key.startswith('$') for key in update_operation.keys()):
            update_operation = {'$set': update_operation}

        if multi:
            result: UpdateResult = collection.update_many(query, update_operation)
        else:
            result: UpdateResult = collection.update_one(query, update_operation)

        return result.modified_count

    except Exception as e:
        logging.error(f"Erreur MongoDB update: {e}", exc_info=True)
        raise

# Fonction pour supprimer des documents
def delete_documents(
    collection: Collection, 
    query: Dict[str, Any], 
    multi: bool = False
) -> int:
    """
    Supprime des documents avec gestion des _id personnalisés
    """
    try:            
        if multi:
            result = collection.delete_many(query)
        else:
            result = collection.delete_one(query)
            
        return result.deleted_count
        
    except Exception as e:
        logging.error(f"Delete error: {str(e)}")
        return 0

# Fonction pour agréger des documents
def aggregate_documents(collection: Collection, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Exécute une agrégation sur une collection MongoDB.
    
    Args:
        collection (Collection): Collection MongoDB
        pipeline (List[Dict[str, Any]]): Pipeline d'agrégation
        
    Returns:
        List[Dict[str, Any]]: Résultats de l'agrégation
    """
    try:
        return list(collection.aggregate(pipeline))
    except Exception as e:
        print(f"Erreur lors de l'agrégation: {str(e)}")
        return [] 