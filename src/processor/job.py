import logging
import argparse
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
from pyspark.sql.functions import expr, lit, to_json, struct

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP = "kafka:29092"
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

# Global variable to hold rules (initialized in main)
ACTIVE_RULES = []

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# helper functions
def get_spark_type(type_str):
    '''
    Convert the string types to actual pyspark datatypes
    '''
    mapping = {
        "string": StringType(),
        "integer": IntegerType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "timestamp": StringType() # We read as string first, then cast
    }
    return mapping.get(type_str.lower(), StringType())

def fetch_schema_from_db(spark, pipeline_id):
    '''
    Get the schema for the pipelines from the db
    '''
    query = f"""
    (SELECT s.schema_json, p.kafka_topic 
     FROM schemas s 
     JOIN pipelines p ON p.id = s.pipeline_id 
     WHERE p.id = {pipeline_id}) as config_alias
    """
    
    config_df = spark.read.format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", query) \
        .option("user", POSTGRES_PROPERTIES["user"]) \
        .option("password", POSTGRES_PROPERTIES["password"]) \
        .option("driver", POSTGRES_PROPERTIES["driver"]) \
        .load()
    
    if config_df.isEmpty():
        raise ValueError(f"No pipeline found for ID {pipeline_id}")
    
    row = config_df.first()
    return row["schema_json"], row["kafka_topic"]

def fetch_rules_from_db(spark, pipeline_id):
    query = f"""
    (SELECT rule_expression, description, severity 
     FROM rules 
     WHERE pipeline_id = {pipeline_id}) as rules_alias
    """
    
    rules_df = spark.read.format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", query) \
        .option("user", POSTGRES_PROPERTIES["user"]) \
        .option("password", POSTGRES_PROPERTIES["password"]) \
        .option("driver", POSTGRES_PROPERTIES["driver"]) \
        .load()
    
    return [row.asDict() for row in rules_df.collect()]

def process_batch(batch_df, batch_id):
    """
    Runs on every micro-batch. 
    1. Archives raw data to R2 (Parquet).
    2. Checks Dynamic Rules.
    3. Writes alerts to Postgres (JSONB).
    """
    if batch_df.isEmpty():
        return

    logger.info(f"Processing Batch ID: {batch_id} with {batch_df.count()} records")
    
    batch_df.cache()

    try:
        # --- LAYER 1: CLOUD ARCHIVING (R2) ---
        if "timestamp" in batch_df.columns:
            logger.info(f"Archiving batch {batch_id} to R2...")
            batch_df.withColumn("date", col("timestamp").cast("date")) \
                .write \
                .mode("append") \
                .partitionBy("date") \
                .parquet(f"s3a://{BUCKET_NAME}/raw_transactions")
            logger.info(f"✅ Archived batch to R2")
        else:
            logger.warning("⚠️ Skipping R2 Archival: Input data missing 'timestamp' column")

        # --- LAYER 2: DYNAMIC RULE ENGINE ---
        all_alerts = None
        
        for rule in ACTIVE_RULES:
            violation_df = batch_df.filter(expr(rule['rule_expression']))

            if violation_df.isEmpty():
                continue
            
            tagged_df = violation_df \
                .withColumn("rule_description", lit(rule['description'])) \
                .withColumn("severity", lit(rule['severity'])) \
                .withColumn("pipeline_id", lit(PIPELINE_ID))

            if all_alerts is None:
                all_alerts = tagged_df
            else:
                all_alerts = all_alerts.union(tagged_df)

        # --- LAYER 3: WRITE ALERTS TO POSTGRES ---
        if all_alerts:
            # Pack all dynamic columns into JSON string
            output_df = all_alerts.select(
                col("pipeline_id"),
                col("severity"),
                col("rule_description"),
                current_timestamp().alias("alert_timestamp"),
                to_json(struct("*")).alias("transaction_data")
            )
            
            count = output_df.count()
            logger.warning(f"⚠️ Writing {count} alerts to DB")
            
            # FIX: Create a temporary view and use SQL CAST to convert string to JSONB
            output_df.createOrReplaceTempView("alerts_temp")
            
            # Use Spark SQL to prepare data, then write with proper SQL statement
            # Option 1: Register temp view and let PostgreSQL handle the cast
            output_df.write \
                .format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", "fraud_alerts") \
                .option("user", POSTGRES_PROPERTIES["user"]) \
                .option("password", POSTGRES_PROPERTIES["password"]) \
                .option("driver", POSTGRES_PROPERTIES["driver"]) \
                .option("stringtype", "unspecified") \
                .mode("append") \
                .save()

    except Exception as e:
        logger.error(f"❌ Error processing batch {batch_id}: {str(e)}", exc_info=True)
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

    # parse arguments to get the pipeline id
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-id", type=int, required=True, help="ID of the pipeline to run")
    args = parser.parse_known_args()[0]

    global PIPELINE_ID 
    PIPELINE_ID = args.pipeline_id

    # fetch the configuration dynamically
    logger.info(f"Fetching configuration for Pipeline ID: {args.pipeline_id}")
    schema_json_str, kafka_topic = fetch_schema_from_db(spark, args.pipeline_id)

    # Parse the JSON string into a Python list
    schema_list = json.loads(schema_json_str)

    # Dynamically Build Spark Schema
    fields = []
    for field in schema_list:
        s_field = StructField(
            field['name'], 
            get_spark_type(field['type']), 
            field['nullable']
        )
        fields.append(s_field)
    
    dynamic_schema = StructType(fields)
    logger.info(f"✅ Dynamic Schema Built: {dynamic_schema.simpleString()}")

    # Fetch Rules
    global ACTIVE_RULES
    ACTIVE_RULES = fetch_rules_from_db(spark, args.pipeline_id)
    logger.info(f"Loaded {len(ACTIVE_RULES)} rules from database.")

    # Read Stream from Kafka
    logger.info(f"Starting Kafka Stream (topic: {kafka_topic}, bootstrap: {KAFKA_BOOTSTRAP})...")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    logger.info("✅ Kafka stream connected")

    # Parse JSON using dynamic schema
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), dynamic_schema).alias("data")
    ).select("data.*")

    logger.info("Starting structured streaming query with 10-second micro-batches...")

    # If your schema has a timestamp field, handle the cast specifically
    if "timestamp" in parsed_df.columns:
        parsed_df = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp")))

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