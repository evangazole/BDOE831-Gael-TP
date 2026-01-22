"""
Script PySpark pour lire les données depuis MinIO (S3).

Ce script configure Spark pour accéder à MinIO et lit les fichiers CSV
en DataFrames pour afficher les schémas et statistiques de base.
"""

from pyspark.sql import SparkSession

# Créer la session Spark avec configuration MinIO
spark = SparkSession.builder \
    .appName("Read from MinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("=" * 80)
print("Lecture des données depuis MinIO")
print("=" * 80)

# Lire Network_logs.csv
print("\n📊 Network_logs.csv")
print("-" * 80)
try:
    df_network = spark.read.csv(
        "s3a://data-lake/logs/Network_logs.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"Nombre de lignes : {df_network.count()}")
    print("\nSchéma :")
    df_network.printSchema()
    print("\nAperçu des données :")
    df_network.show(5, truncate=False)
    print("\nStatistiques descriptives :")
    df_network.describe().show()
except Exception as e:
    print(f"❌ Erreur lors de la lecture : {e}")

# Lire Time-Series_Network_logs.csv
print("\n📊 Time-Series_Network_logs.csv")
print("-" * 80)
try:
    df_timeseries = spark.read.csv(
        "s3a://data-lake/logs/Time-Series_Network_logs.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"Nombre de lignes : {df_timeseries.count()}")
    print("\nSchéma :")
    df_timeseries.printSchema()
    print("\nAperçu des données :")
    df_timeseries.show(5, truncate=False)
except Exception as e:
    print(f"❌ Erreur lors de la lecture : {e}")

# Lire dbip-country-lite
print("\n📊 dbip-country-lite-2026-01.csv")
print("-" * 80)
try:
    df_country = spark.read.csv(
        "s3a://data-lake/reference-data/dbip-country-lite-2026-01.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"Nombre de lignes : {df_country.count()}")
    print("\nSchéma :")
    df_country.printSchema()
    print("\nAperçu des données :")
    df_country.show(5, truncate=False)
except Exception as e:
    print(f"❌ Erreur lors de la lecture : {e}")

# Lire client_hostname.csv
print("\n📊 client_hostname.csv")
print("-" * 80)
try:
    df_client = spark.read.csv(
        "s3a://data-lake/reference-data/client_hostname.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"Nombre de lignes : {df_client.count()}")
    print("\nSchéma :")
    df_client.printSchema()
    print("\nAperçu des données :")
    df_client.show(5, truncate=False)
except Exception as e:
    print(f"❌ Erreur lors de la lecture : {e}")

print("\n" + "=" * 80)
print("✅ Lecture terminée")
print("=" * 80)

spark.stop()
