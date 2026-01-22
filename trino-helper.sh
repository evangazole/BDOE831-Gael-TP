#!/bin/bash
# Script helper pour interagir avec Trino via CLI

echo "=========================================="
echo "Trino CLI Helper - BDOE831"
echo "=========================================="
echo ""

# Fonction pour exécuter une requête SQL
execute_query() {
    local query="$1"
    echo "Exécution de la requête : $query"
    docker exec -it trino trino --server http://localhost:8080 --catalog hive --schema intrusion_detection --execute "$query"
}

# Menu principal
echo "Options disponibles :"
echo "1. Se connecter à Trino CLI (interactif)"
echo "2. Créer les tables (exécuter create_tables.sql)"
echo "3. Lister les tables"
echo "4. Afficher les 10 premières lignes de network_logs_cleaned"
echo "5. Statistiques sur les labels"
echo ""

read -p "Choisissez une option (1-5) : " choice

case $choice in
    1)
        echo "Connexion à Trino CLI..."
        docker exec -it trino trino --server http://localhost:8080 --catalog hive --schema intrusion_detection
        ;;
    2)
        echo "Création des tables..."
        docker exec -i trino trino --server http://localhost:8080 --catalog hive < trino-queries/create_tables.sql
        echo "✓ Tables créées"
        ;;
    3)
        execute_query "SHOW TABLES IN hive.intrusion_detection;"
        ;;
    4)
        execute_query "SELECT * FROM hive.intrusion_detection.network_logs_cleaned LIMIT 10;"
        ;;
    5)
        execute_query "SELECT label, COUNT(*) as count FROM hive.intrusion_detection.network_logs_cleaned GROUP BY label;"
        ;;
    *)
        echo "Option invalide"
        exit 1
        ;;
esac
