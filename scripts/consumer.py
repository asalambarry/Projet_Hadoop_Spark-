from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (FloatType, IntegerType, StructField, StructType,
                               TimestampType)


def create_spark_session():
    """Création de la session Spark"""
    return SparkSession.builder \
        .appName("RealTimeRecommendations") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

def create_schema():
    """Définition du schéma des messages"""
    return StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

def run_consumer():
    """Fonction principale du consumer"""
    # Configuration
    KAFKA_BROKER = 'bigdata-container:9092'
    TOPIC_NAME = 'movieratings'
    MODEL_PATH = "hdfs://namenode:9000/models/als_recommender"

    print("Démarrage du consumer...")
    spark = create_spark_session()
    schema = create_schema()

    # Configuration du streaming
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .load()

    # Transformation et prédictions
    parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    als_model = ALSModel.load(MODEL_PATH)
    predictions = als_model.transform(parsed_stream)

    # Démarrage du streaming
    query = predictions.select("userId", "movieId", "rating") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    try:
        print("En attente des messages...")
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nArrêt du consumer...")
        query.stop()

if __name__ == "__main__":
    run_consumer()