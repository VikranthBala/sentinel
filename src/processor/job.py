import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP = "kafka:29092"  # Internal Docker network address
TOPIC = "transactions"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/fraud_detection_db"
POSTGRES_PROPERTIES = {
    "user": "admin",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# R2 Configuration
BUCKET_NAME = "sentinel"
CHECKPOINT_DIR = "/tmp/spark-checkpoints/sentinel-fraud-detection"

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_batch(batch_df, batch_id):
    """
    This function runs on every micro-batch (e.g., every 10 seconds).
    It persists the data, runs two different analytics, and writes to Postgres.
    """
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id} is empty, skipping...")
        return

    logger.info(f"Processing Batch ID: {batch_id} with {batch_df.count()} records")
    
    # Cache the batch because we will use it multiple times
    batch_df.cache()

    try:
        # --- LAYER 1: CLOUD ARCHIVING (R2) ---
        logger.info(f"Archiving batch {batch_id} to R2...")
        
        batch_df.withColumn("date", col("timestamp").cast("date")) \
            .write \
            .mode("append") \
            .partitionBy("date") \
            .parquet(f"s3a://{BUCKET_NAME}/raw_transactions")
            
        logger.info(f"✅ Archived batch {batch_id} to R2 (s3a://{BUCKET_NAME}/raw_transactions)")

        # --- LOGIC 1: DETECT FRAUD (Simple Rule Engine) ---
        alerts_df = batch_df.filter(col("amount") > 5000)
        
        fraud_count = alerts_df.count()
        if fraud_count > 0:
            logger.warning(f"⚠️ Found {fraud_count} suspicious transactions in batch {batch_id}!")
            
            # Write to PostgreSQL
            alerts_df.select("transaction_id", "user_id", "amount", "timestamp") \
                .write \
                .jdbc(url=POSTGRES_URL, table="fraud_alerts", mode="append", properties=POSTGRES_PROPERTIES)
            
            logger.info(f"✅ Wrote {fraud_count} fraud alerts to PostgreSQL")

        # --- LOGIC 2: AGGREGATE STATS (User Insights) ---
        stats_df = batch_df.groupBy("user_id") \
            .agg({"amount": "avg", "transaction_id": "count"}) \
            .withColumnRenamed("avg(amount)", "avg_spend") \
            .withColumnRenamed("count(transaction_id)", "txn_count")
        
        stats_count = stats_df.count()
        stats_df.write \
            .jdbc(url=POSTGRES_URL, table="user_stats", mode="append", properties=POSTGRES_PROPERTIES)
        
        logger.info(f"✅ Wrote statistics for {stats_count} users to PostgreSQL")

    except Exception as e:
        logger.error(f"❌ Error processing batch {batch_id}: {str(e)}", exc_info=True)
        # Don't raise - allow stream to continue
    finally:
        batch_df.unpersist()
        logger.info(f"Completed processing batch {batch_id}")

def main():
    # Get R2 credentials from environment
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3_endpoint = os.getenv("S3_ENDPOINT")

    # Validate credentials
    if not all([aws_access_key, aws_secret_key, s3_endpoint]):
        raise ValueError(
            "Missing R2 credentials! Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_ENDPOINT"
        )

    logger.info(f"Initializing Spark with R2 endpoint: {s3_endpoint}")

    # Create Spark Session with all required dependencies
    spark = SparkSession.builder \
        .appName("SentinelFraudEngine") \
        .config("spark.jars.packages", 
                "org.postgresql:postgresql:42.6.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Spark Session created successfully")

    # Define Schema matching Go Producer
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("amount", FloatType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # Read Stream from Kafka
    logger.info(f"Starting Kafka Stream (topic: {TOPIC}, bootstrap: {KAFKA_BOOTSTRAP})...")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    logger.info("✅ Kafka stream connected")

    # Parse JSON
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*") \
     .withColumn("timestamp", to_timestamp(col("timestamp")))

    logger.info("Starting structured streaming query with 10-second micro-batches...")

    # Start Processing with Checkpointing
    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .start()

    logger.info(f"✅ Streaming query started (checkpoint: {CHECKPOINT_DIR})")
    logger.info("Press Ctrl+C to stop the application")

    query.awaitTermination()

if __name__ == "__main__":
    main()