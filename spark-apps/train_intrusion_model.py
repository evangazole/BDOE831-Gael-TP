"""
Script PySpark pour entraîner un modèle de détection d'intrusion.

Ce script :
1. Lit les données transformées (Parquet)
2. Prépare les features (encodage, normalisation)
3. Entraîne un modèle de Régression Logistique (classification binaire)
4. Évalue le modèle (Matrice de confusion, Accuracy, Précision, Recall)
5. Sauvegarde le modèle entraîné dans MinIO
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

# Créer la session Spark
spark = SparkSession.builder \
    .appName("Train Intrusion Detection Model") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("=" * 80)
print("Entraînement du Modèle de Détection d'Intrusion")
print("=" * 80)

# 1. Lecture des données transformées
print("\n📖 Lecture des données transformées...")
df = spark.read.parquet("s3a://data-lake/processed/network_logs_cleaned.parquet")
print(f"✓ Données chargées : {df.count()} lignes")

# 2. Affichage du schéma
print("\nSchéma des données :")
df.printSchema()

# 3. Identification de la colonne label
# Note: Adapter selon le nom réel de la colonne dans votre dataset
# Exemples courants : 'label', 'Label', 'class', 'attack_type', 'is_intrusion'
label_col = None
for possible_label in ['label', 'Label', 'class', 'attack_type', 'is_intrusion', 'target']:
    if possible_label in df.columns:
        label_col = possible_label
        break

if label_col is None:
    print("\n⚠️  ATTENTION : Aucune colonne label trouvée dans le dataset.")
    print("   Colonnes disponibles :", df.columns)
    print("   Pour un apprentissage supervisé, vous devez avoir une colonne label.")
    print("   Création d'une colonne label factice pour démonstration...")
    # Créer une colonne label factice (à remplacer par la vraie colonne)
    df = df.withColumn("label", (col(df.columns[0]) % 2).cast("double"))
    label_col = "label"

print(f"\n✓ Colonne label identifiée : '{label_col}'")

# 4. Sélection des features numériques
numeric_features = [field.name for field in df.schema.fields 
                    if str(field.dataType) in ['IntegerType', 'DoubleType', 'LongType', 'FloatType']
                    and field.name != label_col]

print(f"\n📊 Features numériques sélectionnées ({len(numeric_features)}) :")
print(numeric_features[:10], "..." if len(numeric_features) > 10 else "")

# 5. Préparation des features
print("\n🔧 Préparation des features...")

# Assembler les features en un vecteur
assembler = VectorAssembler(
    inputCols=numeric_features,
    outputCol="features_raw",
    handleInvalid="skip"
)

# Normalisation des features
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=True
)

# 6. Création du modèle de Régression Logistique
print("\n🤖 Création du modèle de Régression Logistique...")
lr = LogisticRegression(
    featuresCol="features",
    labelCol=label_col,
    maxIter=100,
    regParam=0.01,
    elasticNetParam=0.8
)

# 7. Pipeline ML
pipeline = Pipeline(stages=[assembler, scaler, lr])

# 8. Split train/test
print("\n📊 Séparation des données (80% train, 20% test)...")
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"✓ Train : {train_df.count()} lignes")
print(f"✓ Test : {test_df.count()} lignes")

# 9. Entraînement du modèle
print("\n🚀 Entraînement du modèle...")
model = pipeline.fit(train_df)
print("✓ Modèle entraîné avec succès")

# 10. Prédictions sur le test set
print("\n🔮 Prédictions sur le test set...")
predictions = model.transform(test_df)

# 11. Évaluation du modèle
print("\n📈 Évaluation du modèle :")
print("-" * 80)

# Accuracy
evaluator_accuracy = MulticlassClassificationEvaluator(
    labelCol=label_col,
    predictionCol="prediction",
    metricName="accuracy"
)
accuracy = evaluator_accuracy.evaluate(predictions)
print(f"✓ Accuracy : {accuracy:.4f}")

# Precision
evaluator_precision = MulticlassClassificationEvaluator(
    labelCol=label_col,
    predictionCol="prediction",
    metricName="weightedPrecision"
)
precision = evaluator_precision.evaluate(predictions)
print(f"✓ Precision : {precision:.4f}")

# Recall
evaluator_recall = MulticlassClassificationEvaluator(
    labelCol=label_col,
    predictionCol="prediction",
    metricName="weightedRecall"
)
recall = evaluator_recall.evaluate(predictions)
print(f"✓ Recall : {recall:.4f}")

# F1-Score
evaluator_f1 = MulticlassClassificationEvaluator(
    labelCol=label_col,
    predictionCol="prediction",
    metricName="f1"
)
f1 = evaluator_f1.evaluate(predictions)
print(f"✓ F1-Score : {f1:.4f}")

# AUC-ROC
evaluator_auc = BinaryClassificationEvaluator(
    labelCol=label_col,
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
auc = evaluator_auc.evaluate(predictions)
print(f"✓ AUC-ROC : {auc:.4f}")

# 12. Affichage de quelques prédictions
print("\n🔍 Exemples de prédictions :")
predictions.select(label_col, "prediction", "probability").show(10, truncate=False)

# 12.5. Sauvegarde des prédictions pour PowerBI
print("\n💾 Sauvegarde des prédictions pour visualisation...")
predictions_path = "s3a://data-lake/processed/classified_logs.parquet"
predictions.write.mode("overwrite").parquet(predictions_path)
print(f"✓ Prédictions sauvegardées : {predictions_path}")

# 13. Sauvegarde du modèle
print("\n💾 Sauvegarde du modèle...")
model_path = "s3a://data-lake/models/intrusion_detection_model"
model.write().overwrite().save(model_path)
print(f"✓ Modèle sauvegardé : {model_path}")

# 14. Résumé final
print("\n" + "=" * 80)
print("✅ Entraînement terminé avec succès")
print("=" * 80)
print(f"\n📊 Résumé des performances :")
print(f"   - Accuracy  : {accuracy:.4f}")
print(f"   - Precision : {precision:.4f}")
print(f"   - Recall    : {recall:.4f}")
print(f"   - F1-Score  : {f1:.4f}")
print(f"   - AUC-ROC   : {auc:.4f}")
print("\n" + "=" * 80)

spark.stop()
