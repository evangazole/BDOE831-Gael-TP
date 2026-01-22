# Script R pour charger les données depuis MinIO dans Power BI
# Prérequis : Le package 'arrow' doit être installé dans votre environnement R.
# Exécutez : install.packages("arrow") dans RStudio ou votre console R avant d'utiliser ce script.

library(arrow)

# Configuration de la connexion S3 vers MinIO
# Note: "use_ssl = FALSE" est important pour une connexion HTTP locale
bucket <- s3_bucket(
  "data-lake",
  endpoint_override = "http://localhost:9010",
  access_key = "minioadmin",
  secret_key = "minioadmin",
  scheme = "http" 
)

# Chemin vers le fichier Parquet généré par Spark
# Assurez-vous que le chemin correspond exactement à celui dans MinIO
parquet_path <- "processed/classified_logs.parquet"

# Lecture du fichier Parquet directement dans un DataFrame
dataset <- read_parquet(bucket$path(parquet_path))

# Suppression des colonnes complexes si nécessaire (les types vecteur de Spark peuvent poser problème à PowerBI)
# Ici on garde tout, PowerBI gérera au mieux.
