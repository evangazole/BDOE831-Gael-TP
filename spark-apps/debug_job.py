from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Debug Schema") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("\nInspecting network_logs_cleaned.parquet...")
path = "s3a://data-lake/processed/network_logs_cleaned.parquet"
df = spark.read.parquet(path)
df.printSchema()
df.show(5)

print("\nChecking for potential labels...")
for col_name in df.columns:
    if "label" in col_name.lower() or "class" in col_name.lower() or "intrusion" in col_name.lower():
        print(f"Distribution for {col_name}:")
        df.groupBy(col_name).count().show()

print("\nNumeric features summary:")
numeric_features = [field.name for field in df.schema.fields 
                    if str(field.dataType) in ['IntegerType', 'DoubleType', 'LongType', 'FloatType']]
print(f"Numeric features found: {numeric_features}")
df.select(numeric_features).describe().show()

spark.stop()
