# streaming_recommendations.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, array, lit, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.ml.recommendation import ALSModel
import time
import json
from pymongo import MongoClient
import datetime

# Créer une session Spark avec support Kafka et MongoDB
spark = SparkSession.builder \
    .appName("MovieRecommendationStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Définir le schéma des données entrantes
schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", IntegerType(), True)
])

# Charger le modèle ALS préalablement entraîné
model_path = "hdfs://namenode:9000/models/als_recommender"
model = ALSModel.load(model_path)

# Configuration MongoDB
mongo_uri = "mongodb://localhost:27017"
mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client["movie_recommender"]
mongo_collection = mongo_db["recommendations"]

# Fonction pour générer des recommandations et les sauvegarder dans MongoDB
def process_batch(df, epoch_id):
    if not df.isEmpty():
        try:
            # Extraire les userId uniques du batch
            unique_users = df.select("userId").distinct()
            
            # Générer des recommandations pour chaque utilisateur
            recommendations = model.recommendForUserSubset(unique_users, 10)
            
            # Exploser les recommandations pour avoir un format plat
            flat_recommendations = recommendations.select(
                col("userId"),
                explode(col("recommendations")).alias("rec")
            ).select(
                col("userId"),
                col("rec.movieId").alias("movieId"),
                col("rec.rating").alias("prediction")
            )
            
            # Joindre avec les informations de films (si disponible)
            try:
                movies_df = spark.read.csv("hdfs://namenode:9000/datasets/movies.csv", header=True)
                recommendations_with_info = flat_recommendations.join(
                    movies_df, flat_recommendations.movieId == movies_df.movieId
                ).select(
                    flat_recommendations.userId,
                    flat_recommendations.movieId,
                    movies_df.title,
                    flat_recommendations.prediction
                )
            except:
                recommendations_with_info = flat_recommendations
            
            # Afficher les recommandations
            recommendations_with_info.show(10, False)
            
            # Convertir le DataFrame en liste de dictionnaires pour MongoDB
            recommendations_list = recommendations_with_info.withColumn(
                "timestamp", lit(datetime.datetime.now().isoformat())
            ).toJSON().collect()
            
            # Insérer dans MongoDB
            if recommendations_list:
                documents = [json.loads(rec) for rec in recommendations_list]
                mongo_collection.insert_many(documents)
                
            print(f"Batch {epoch_id}: Recommandations générées et sauvegardées pour {unique_users.count()} utilisateurs")
        
        except Exception as e:
            print(f"Erreur lors du traitement du batch {epoch_id}: {str(e)}")

# Lire les données du stream Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "MoviesRatings") \
    .option("startingOffsets", "latest") \
    .load()

# Transformer les données JSON en DataFrame structuré
parsed_stream = kafka_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Traiter les données par batch
query = parsed_stream \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .trigger(processingTime="10 seconds") \
    .start()

# Attendre la fin du traitement
query.awaitTermination()