# 📦 Projet Data Ingestion - Cryptomonnaies

## 🏗️ Architecture du Projet

Ce projet met en place une pipeline d’ingestion et de traitement de données de cryptomonnaies à l’aide de conteneurs Docker.  
Il comprend les composants suivants :

- **Scripts Python** : collectent les données de marché, réalisent des traitements batch et streaming.
- **Apache Cassandra** : base NoSQL pour stocker les données traitées.
- **Docker Compose** : pour gérer et orchestrer l’ensemble des services.
![image](https://github.com/user-attachments/assets/7cd8a9d1-7dcd-4864-a3e1-e07cf0967b5f)


### 📁 Organisation des Données (Cassandra)

Les données sont stockées dans le **keyspace `crypto_keyspace`** avec les tables suivantes :

- `crypto_raw` : données brutes collectées.
- `crypto_batch_aggregation` : agrégats de données (moyenne, somme, etc.).
- `crypto_batch_correlation` : corrélations entre cryptomonnaies.
- `crypto_streaming` : données collectées en temps réel via streaming.

---

## 🚀 Lancer le Projet

Dans un terminal, exécutez les commandes suivantes :

```bash
cd data_ingestion
docker compose up -d --build
