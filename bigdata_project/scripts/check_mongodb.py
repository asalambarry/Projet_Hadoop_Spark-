# check_mongodb.py
from pymongo import MongoClient
import pprint

# Connexion à MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['movie_recommender']
collection = db['recommendations']

# Afficher les dernières recommandations
print("Dernières recommandations sauvegardées:")
cursor = collection.find().sort('timestamp', -1).limit(10)
for doc in cursor:
    pprint.pprint(doc)

# Statistiques
count = collection.count_documents({})
user_count = len(collection.distinct('userId'))
movie_count = len(collection.distinct('movieId'))

print(f"\nStatistiques:")
print(f"- Nombre total de recommandations: {count}")
print(f"- Nombre d'utilisateurs uniques: {user_count}")
print(f"- Nombre de films uniques recommandés: {movie_count}")

# Utilisateurs avec le plus de recommandations
print("\nUtilisateurs avec le plus de recommandations:")
pipeline = [
    {"$group": {"_id": "$userId", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 5}
]
for doc in collection.aggregate(pipeline):
    print(f"- Utilisateur {doc['_id']}: {doc['count']} recommandations")