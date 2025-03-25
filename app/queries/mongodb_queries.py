"""
Module contenant les opérations CRUD pour MongoDB
"""
from typing import List, Dict, Any, Optional
from pymongo.collection import Collection
from pymongo.results import InsertOneResult, UpdateResult, DeleteResult

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

def find_documents(collection: Collection, query: Dict[str, Any] = None, 
                  projection: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """
    Recherche des documents dans une collection MongoDB.
    
    Args:
        collection (Collection): Collection MongoDB
        query (Dict[str, Any], optional): Critères de recherche
        projection (Dict[str, Any], optional): Champs à retourner
        
    Returns:
        List[Dict[str, Any]]: Liste des documents trouvés
    """
    try:
        cursor = collection.find(query or {}, projection or {})
        return list(cursor)
    except Exception as e:
        print(f"Erreur lors de la recherche: {str(e)}")
        return []

def update_documents(collection: Collection, query: Dict[str, Any],
                    update: Dict[str, Any]) -> int:
    """
    Met à jour des documents dans une collection MongoDB.
    
    Args:
        collection (Collection): Collection MongoDB
        query (Dict[str, Any]): Critères de recherche
        update (Dict[str, Any]): Modifications à appliquer
        
    Returns:
        int: Nombre de documents modifiés
    """
    try:
        result: UpdateResult = collection.update_many(query, {"$set": update})
        return result.modified_count
    except Exception as e:
        print(f"Erreur lors de la mise à jour: {str(e)}")
        return 0

def delete_documents(collection: Collection, query: Dict[str, Any]) -> int:
    """
    Supprime des documents d'une collection MongoDB.
    
    Args:
        collection (Collection): Collection MongoDB
        query (Dict[str, Any]): Critères de suppression
        
    Returns:
        int: Nombre de documents supprimés
    """
    try:
        result: DeleteResult = collection.delete_many(query)
        return result.deleted_count
    except Exception as e:
        print(f"Erreur lors de la suppression: {str(e)}")
        return 0

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