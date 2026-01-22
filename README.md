#  Résumé du Projet : Détection d'Intrusion Big Data

Ce projet implémente une architecture **Lakehouse** complète pour collecter, traiter et visualiser des logs réseau afin de détecter des comportements suspects à l'aide du Machine Learning.

##  L'Architecture (Docker)

Le projet utilise 5 composants principaux qui tournent dans des conteneurs :

1.  **MinIO (S3)** : Le "cœur" du stockage. Tous les fichiers (bruts, nettoyés, modèles ML) sont stockés ici.
2.  **Apache Airflow** : L'orchestrateur. Il s'occupe de prendre les fichiers CSV locaux et de les envoyer automatiquement dans MinIO.
3.  **Apache Spark** : Le moteur de calcul. Il nettoie les données et entraîne le modèle de Machine Learning (Régression Logistique).
4.  **Trino** : Le moteur SQL. Il permet de requêter les fichiers dans MinIO comme s'ils étaient dans une base de données classique.
5.  **Power BI / R** : La couche de visualisation pour créer des graphiques et surveiller les intrusions.

##  Flux de données (End-to-End)

1.  **Ingestion** : Airflow détecte les nouveaux logs CSV et les charge dans `s3://data-lake/logs/`.
2.  **Traitement** : Spark lit ces fichiers, supprime les doublons, gère les valeurs manquantes et sauvegarde le résultat au format **Parquet** (plus rapide).
3.  **Machine Learning** : Un second script Spark entraîne un modèle sur les données passées pour apprendre à reconnaître une intrusion. Il génère ensuite des prédictions sur les nouveaux logs (`classified_logs.parquet`).
4.  **Visualisation** : Power BI récupère ces prédictions via un script **R** pour afficher le taux d'intrusion et les alertes.

##  Comment ça marche ?

- On utilise des scripts **Python/PySpark** pour la logique de données.
- On utilise un **Docker-Compose** pour lancer toute l'infrastructure en une commande.
- On utilise le format **Parquet** pour optimiser le stockage et la vitesse des requêtes.

