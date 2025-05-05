import json
import random
import time

from kafka import KafkaProducer

# Configuration Kafka
KAFKA_BROKER = 'bigdata-container:9092'
TOPIC_NAME = 'movieratings'

def create_producer():
    """Création du producer Kafka"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_rating():
    """Génération d'une note aléatoire"""
    return {
        'userId': random.randint(1, 1000),
        'movieId': random.randint(1, 1000),
        'rating': round(random.uniform(0.5, 5.0), 1),
        'timestamp': int(time.time())
    }

def run_producer():
    """Fonction principale du producer"""
    producer = create_producer()
    print("Démarrage du producer - Envoi continu de messages...")

    try:
        while True:  # Boucle infinie pour un envoi continu
            message = generate_rating()
            producer.send(TOPIC_NAME, message)
            print(f"Message envoyé: {message}")
            time.sleep(1)  # Délai d'une seconde entre chaque message
    except KeyboardInterrupt:
        print("\nArrêt du producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()