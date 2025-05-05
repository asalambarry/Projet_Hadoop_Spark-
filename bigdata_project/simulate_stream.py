# simulate_stream.py
from kafka import KafkaProducer
import json
import time
import random
import csv
from datetime import datetime

# Charger les IDs de films réels si disponible
try:
    real_movies = []
    with open('movies.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            real_movies.append(int(row['movieId']))
    movies = real_movies if real_movies else list(range(1, 500))
except:
    movies = list(range(1, 500))

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Données simulées
users = list(range(1, 100))

# Logs des messages
log_file = f"producer_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

# Boucle d'envoi des messages
try:
    count = 0
    with open(log_file, 'w') as log:
        while True:
            # Simuler une interaction utilisateur
            data = {
                "userId": random.choice(users),
                "movieId": random.choice(movies),
                "rating": round(random.uniform(1, 5), 1),
                "timestamp": int(time.time())
            }
            
            # Envoyer au topic Kafka
            producer.send('MoviesRatings', data)
            
            # Log du message
            log_entry = f"[{datetime.now().isoformat()}] Message envoyé: {data}\n"
            log.write(log_entry)
            log.flush()
            
            print(f"Sent: {data}")
            count += 1
            
            # Afficher des statistiques périodiquement
            if count % 50 == 0:
                print(f"Total des messages envoyés: {count}")
            
            # Pause aléatoire pour simuler un trafic plus réaliste
            time.sleep(random.uniform(0.5, 2.0))
            
except KeyboardInterrupt:
    print(f"\nProduction interrompue. Total des messages envoyés: {count}")
    print(f"Journal des messages sauvegardé dans: {log_file}")
    producer.close()