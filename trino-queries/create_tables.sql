-- Script SQL pour créer les tables Hive dans Trino
-- Ce script permet d'exposer les données stockées dans MinIO via Trino

-- 1. Créer la base de données
CREATE SCHEMA IF NOT EXISTS hive.intrusion_detection
WITH (location = 's3a://data-lake/');

-- 2. Table pour les logs réseau bruts
CREATE TABLE IF NOT EXISTS hive.intrusion_detection.network_logs_raw (
    -- Adapter les colonnes selon votre fichier Network_logs.csv
    -- Exemple de structure (à modifier selon vos données réelles)
    source_ip VARCHAR,
    destination_ip VARCHAR,
    source_port INTEGER,
    destination_port INTEGER,
    protocol VARCHAR,
    timestamp TIMESTAMP,
    bytes_sent BIGINT,
    bytes_received BIGINT,
    duration DOUBLE,
    label VARCHAR
)
WITH (
    external_location = 's3a://data-lake/logs/',
    format = 'CSV',
    skip_header_line_count = 1
);

-- 3. Table pour les données transformées (Parquet)
CREATE TABLE IF NOT EXISTS hive.intrusion_detection.network_logs_cleaned (
    -- Les colonnes seront inférées automatiquement depuis le Parquet
)
WITH (
    external_location = 's3a://data-lake/processed/network_logs_cleaned.parquet',
    format = 'PARQUET'
);

-- 3.5. Table pour les prédictions (pour PowerBI)
CREATE TABLE IF NOT EXISTS hive.intrusion_detection.classified_logs (
    -- Les colonnes seront inférées automatiquement du Parquet
)
WITH (
    external_location = 's3a://data-lake/processed/classified_logs.parquet',
    format = 'PARQUET'
);

-- 4. Table pour les données de référence IP → Pays
CREATE TABLE IF NOT EXISTS hive.intrusion_detection.ip_country_mapping (
    ip_start VARCHAR,
    ip_end VARCHAR,
    country_code VARCHAR,
    country_name VARCHAR
)
WITH (
    external_location = 's3a://data-lake/reference-data/',
    format = 'CSV',
    skip_header_line_count = 1
);

-- 5. Vérification des tables créées
SHOW TABLES IN hive.intrusion_detection;

-- 6. Exemple de requêtes analytiques

-- Compter le nombre de logs par label
SELECT label, COUNT(*) as count
FROM hive.intrusion_detection.network_logs_cleaned
GROUP BY label
ORDER BY count DESC;

-- Statistiques sur les ports de destination
SELECT destination_port, COUNT(*) as frequency
FROM hive.intrusion_detection.network_logs_cleaned
GROUP BY destination_port
ORDER BY frequency DESC
LIMIT 10;

-- Distribution des protocoles
SELECT protocol, COUNT(*) as count
FROM hive.intrusion_detection.network_logs_cleaned
GROUP BY protocol;
