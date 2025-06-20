from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
    .appName("YouTubeTrendAnalysis") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .getOrCreate()

# Đọc dữ liệu từ S3
raw_data = spark.read.json("s3a://youtube-trend-data/raw/*.json")

# Chuyển đổi và làm sạch
transformed_data = raw_data.select(
    col("id"),
    col("title"),
    col("category_id"),
    col("category_name"),
    col("views").cast("integer"),
    col("likes").cast("integer"),
    col("comments").cast("integer"),
    col("region"),
    col("published_at").cast("timestamp").alias("published_at"),
    col("timestamp")
).filter(col("title").isNotNull())

transformed_data = transformed_data.orderBy(col("published_at").desc())
# Tính tỷ lệ tương tác
transformed_data = transformed_data.withColumn(
    "engagement_ratio", when(col("views") > 0, col("likes") / col("views")).otherwise(0)
)

# Lưu dữ liệu Parquet
transformed_data.write.mode("append").parquet("s3a://youtube-trend-data/transformed/")