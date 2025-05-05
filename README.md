# Big Data Pseudo-distributed Environment with Hadoop, Spark, Kafka, Python, and Jupyter
 
## 🌍 Project Overview
This project provides a ready-to-use Dockerized environment to work with:
- **Hadoop 3.3.6** (pseudo-distributed)
- **Spark 3.5.1** (standalone mode)
- **Kafka 3.6.1** (with Zookeeper)
- **Python 3** + **PySpark**
- **Jupyter Notebook**
 
## 📊 Architecture
- Hadoop HDFS for distributed file storage (single-node setup)
- Spark for batch and streaming data processing
- Kafka for streaming ingestion
- Python environment with Jupyter for development and experimentation
 
## 🔧 Project Structure
```
/
|-- Dockerfile
|-- docker-compose.yml
|-- Makefile
|-- requirements.txt
|-- config/
|   |-- hadoop/
|       |-- core-site.xml
|       |-- hdfs-site.xml
|       |-- mapred-site.xml
|       |-- yarn-site.xml
|-- notebooks/
|   |-- spark_kafka_demo.ipynb
 
|-- scripts/
    |-- spark_batch_csv_count.py
```
 
## 🔄 Quick Start
 
### 1. Build the Docker Image
```bash
make build
```
 
### 2. Launch the Environment
```bash
make up
```
 
This will start:
- Hadoop HDFS & YARN
- Kafka + Zookeeper
- Jupyter Notebook (accessible on http://localhost:8888)
 
### 3. Access the Container
```bash
docker exec -it bigdata-container
```
 
### 4. Shut Down
```bash
docker compose down
```
 
### 5. Clean Everything (containers, images, volumes)
```bash
docker compose prune -f
```
 
## 📄 Notebooks & Scripts
- **load_datasets.ipynb** : Chargement et traitement des données (Affichage, valeurs manquantes et doublons, etc ...)
- **entrainement_model.ipynb** Application du models Spark MLib et sauvegarde dans HDFS
- **producer_notebook.ipynb** producer Kafka qui génère des données simulant de nouvelles évaluations de films par les utilisateurs.
- **consumer_notebook.ipynb** consumer lit ces messages pour les traiter (ex : les stocker, faire une action, etc.).
-- **app.py** Développement backend dashboard de l'application
 
## Structure des Dossiers
-`datasets` : contient les fichiers csv(movies et rating)
-`notebooks` : Notebooks pour l'analyse et le traitement
-`static et template` : Fichiers pour l'interface
 
## Acces à l'application'
- **Application Flask** : http://localhost:5001/
- **Jupyter Notebook** http://localhost:8888
- **Hadoop Namenode**  http://localhost:9870
 
## 🔔 Notes
- Hadoop HDFS Web UI: [http://localhost:9870](http://localhost:9870)
- Ensure you manually create Kafka topics using:
  ```bash
  kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092
  ```
- Upload datasets to HDFS:
  ```bash
  hdfs dfs -mkdir -p /datasets
  hdfs dfs -put your_file.csv /datasets/
  ```
 
---
## Gestion d'equipe
- **L'organisation des tâches avec Trello**: https://trello.com/b/GXT333n6/projetrecommovielens
 
Trello
Organize anything, together. Trello is a collaboration tool that organizes your projects into boards. In one glance, know what's being worked on, who's working on what, and where something is in a ...
 
