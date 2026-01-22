#!/bin/bash
set -e

# Création de la base de données metastore si elle n'existe pas
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE metastore'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metastore')\gexec
EOSQL
