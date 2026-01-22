"""
DAG Airflow pour l'ingestion automatique des fichiers CSV vers MinIO.

Ce DAG scanne le répertoire local pour tous les fichiers CSV et les uploade
vers le bucket MinIO 'data-lake' avec une organisation par type.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import glob

# Configuration par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définition du DAG
dag = DAG(
    'ingest_csv_to_minio',
    default_args=default_args,
    description='Ingestion automatique des fichiers CSV vers MinIO',
    schedule_interval='@daily',
    catchup=False,
    tags=['ingestion', 'minio', 'csv'],
)


def upload_csv_to_minio(**context):
    """
    Upload tous les fichiers CSV du répertoire local vers MinIO.
    """
    # Configuration MinIO via S3Hook
    s3_hook = S3Hook(
        aws_conn_id='minio_s3',
        verify=False
    )
    
    bucket_name = 'data-lake'
    
    # Créer le bucket s'il n'existe pas
    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' créé avec succès")
    
    # Répertoire source des fichiers CSV
    # Note: Ajuster ce chemin selon votre configuration
    csv_directory = '/opt/airflow/data'  # Monter ce volume dans docker-compose
    
    # Rechercher tous les fichiers CSV
    csv_files = glob.glob(os.path.join(csv_directory, '*.csv'))
    
    if not csv_files:
        print(f"Aucun fichier CSV trouvé dans {csv_directory}")
        return
    
    print(f"Fichiers CSV trouvés : {len(csv_files)}")
    
    # Upload de chaque fichier
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        
        # Organisation par type de fichier
        if 'Network_logs' in filename or 'Time-Series' in filename:
            s3_key = f'logs/{filename}'
        elif 'dbip-country' in filename or 'client_hostname' in filename:
            s3_key = f'reference-data/{filename}'
        else:
            s3_key = f'raw/{filename}'
        
        # Upload vers MinIO
        s3_hook.load_file(
            filename=csv_file,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        
        print(f"✓ Uploadé : {filename} → s3://{bucket_name}/{s3_key}")
    
    print(f"Ingestion terminée : {len(csv_files)} fichiers uploadés")


# Tâche d'upload
upload_task = PythonOperator(
    task_id='upload_csv_files',
    python_callable=upload_csv_to_minio,
    provide_context=True,
    dag=dag,
)

upload_task
