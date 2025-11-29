import logging
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

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_batch(batch_df, batch_id):
    """
    This function runs on every micro-batch (e.g., every 10 seconds).
    It persists the data, runs two different analytics, and writes to Postgres.
    """
    if batch_df.isEmpty():
        return

    logger.info(f"Processing Batch ID: {batch_id} with {batch_df.count()} records")
    
    # 1. Cache the batch because we will use it twice
    batch_df.cache()

    try:
        # --- LOGIC 1: DETECT FRAUD (Simple Rule Engine) ---
        # Flag transactions > $5000 as 'SUSPICIOUS'
        # FIX: Removed the buggy .withColumn("reason"...) line
        alerts_df = batch_df.filter(col("amount") > 5000)
        
        # Check if we have any fraud in this batch
        if alerts_df.count() > 0:
            logger.warning(f"⚠️ Found {alerts_df.count()} suspicious transactions!")
            
            # Select only columns that match your Postgres table
            alerts_df.select("transaction_id", "user_id", "amount", "timestamp") \
                .write \
                .jdbc(url=POSTGRES_URL, table="fraud_alerts", mode="append", properties=POSTGRES_PROPERTIES)

        # --- LOGIC 2: AGGREGATE STATS (User Insights) ---
        # Calculate running average for this batch
        stats_df = batch_df.groupBy("user_id").agg({"amount": "avg", "transaction_id": "count"}) \
            .withColumnRenamed("avg(amount)", "avg_spend") \
            .withColumnRenamed("count(transaction_id)", "txn_count")
        
        # Write stats to a separate table
        stats_df.write \
            .jdbc(url=POSTGRES_URL, table="user_stats", mode="append", properties=POSTGRES_PROPERTIES)

    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {e}")
    finally:
        batch_df.unpersist()

def main():
    spark = SparkSession.builder \
        .appName("SentinelFraudEngine") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Define Schema matching your Go Producer
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("amount", FloatType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # 1. Read Stream from Kafka
    logger.info("Starting Kafka Stream...")

    """
        The following gives you a streaming DataFrame with columns:
        1. key
        2. value
        3. topic
        4. partition
        5. offset
        6. timestamp
    
        value (the actual message) is binary by default.
    """
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Parse JSON
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*") \
     .withColumn("timestamp", to_timestamp(col("timestamp"))) # Convert string to proper timestamp

    # 3. Start Processing (Micro-Batch Trigger every 10 seconds)
    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()