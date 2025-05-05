from pymongo import MongoClient

# Test depuis l'intérieur du conteneur avec authentification
client = MongoClient('mongodb://root:example@mongodb:27017/')

try:
    # Envoi d'une commande ping pour confirmer la connexion
    client.admin.command('ping')
    print("Connexion à MongoDB réussie!")

    # Créer une base de données test
    db = client['testdb']
    collection = db['test']

    # Insérer un document
    result = collection.insert_one({"test": "Hello MongoDB!"})
    print(f"Document inséré avec succès! ID: {result.inserted_id}")

    # Lire le document inséré
    doc = collection.find_one({"test": "Hello MongoDB!"})
    print(f"Document lu : {doc}")

except Exception as e:
    print(f"Erreur de connexion: {e}")