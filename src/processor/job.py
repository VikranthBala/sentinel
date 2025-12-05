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

def get_pipeline_config(spark, pipeline_id):
    """
    Fetches both status and rules in one go to minimize DB hits.
    """
    # Check Pipeline Status
    status_query = f"(SELECT status FROM pipelines WHERE id = {pipeline_id}) as status_alias"
    status_df = spark.read.jdbc(url=POSTGRES_URL, table=status_query, properties=POSTGRES_PROPERTIES)
    
    if status_df.isEmpty():
        return "inactive", []
    
    current_status = status_df.first()['status']
    
    # Check Active Rules
    rules_query = f"""
    (SELECT rule_expression, description, severity 
     FROM rules 
     WHERE pipeline_id = {pipeline_id} AND is_active = TRUE) as rules_alias
    """
    rules_df = spark.read.jdbc(url=POSTGRES_URL, table=rules_query, properties=POSTGRES_PROPERTIES)
    rules = [row.asDict() for row in rules_df.collect()]
    
    return current_status, rules

def process_batch(batch_df, batch_id):
    """
    Runs on every micro-batch. 
    1. Fetches latest Rules & Status from DB (Dynamic Updates).
    2. Archives raw data to R2 (Always runs if data exists).
    3. Checks Pipeline Status (If paused, skips logic).
    4. Runs Dynamic Rules & Writes Alerts.
    """
    if batch_df.isEmpty():
        return

    # Access the active SparkSession from the DataFrame
    spark = batch_df.sparkSession
    logger.info(f"Processing Batch ID: {batch_id} with {batch_df.count()} records")
    
    batch_df.cache()

    try:
        # --- LAYER 0: FETCH DYNAMIC CONFIGURATION ---
        # We fetch this every batch to allow "hot reloading" of rules/status
        
        # A. Check Pipeline Status
        status_query = f"(SELECT status FROM pipelines WHERE id = {PIPELINE_ID}) as status_alias"
        status_df = spark.read.format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", status_query) \
            .option("user", POSTGRES_PROPERTIES["user"]) \
            .option("password", POSTGRES_PROPERTIES["password"]) \
            .option("driver", POSTGRES_PROPERTIES["driver"]) \
            .load()
            
        # Default to 'inactive' if not found, otherwise get value
        pipeline_status = status_df.first()['status'] if not status_df.isEmpty() else 'inactive'

        # B. Fetch Only ACTIVE Rules
        rules_query = f"""
        (SELECT rule_expression, description, severity 
         FROM rules 
         WHERE pipeline_id = {PIPELINE_ID} AND is_active = TRUE) as rules_alias
        """
        rules_df = spark.read.format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", rules_query) \
            .option("user", POSTGRES_PROPERTIES["user"]) \
            .option("password", POSTGRES_PROPERTIES["password"]) \
            .option("driver", POSTGRES_PROPERTIES["driver"]) \
            .load()
            
        active_rules = [row.asDict() for row in rules_df.collect()]

        # --- LAYER 1: CLOUD ARCHIVING (R2) ---
        # We always archive data, even if the pipeline logic is paused, to prevent data loss.
        if "timestamp" in batch_df.columns:
            # logger.info(f"Archiving batch {batch_id} to R2...")
            batch_df.withColumn("date", col("timestamp").cast("date")) \
                .write \
                .mode("append") \
                .partitionBy("date") \
                .parquet(f"s3a://{BUCKET_NAME}/raw_transactions")
        else:
            logger.warning("⚠️ Skipping R2 Archival: Input data missing 'timestamp' column")

        # --- LAYER 2: LOGIC GATE ---
        if pipeline_status != 'active':
            logger.warning(f"🛑 Pipeline {PIPELINE_ID} is PAUSED. Skipping Rule Engine.")
            return

        if not active_rules:
            logger.info(f"No active rules found for Pipeline {PIPELINE_ID}.")
            return

        # --- LAYER 3: DYNAMIC RULE ENGINE ---
        all_alerts = None
        
        for rule in active_rules:
            # Apply the SQL expression from the DB
            violation_df = batch_df.filter(expr(rule['rule_expression']))

            if violation_df.isEmpty():
                continue
            
            # Tag the data with rule metadata
            tagged_df = violation_df \
                .withColumn("rule_description", lit(rule['description'])) \
                .withColumn("severity", lit(rule['severity'])) \
                .withColumn("pipeline_id", lit(PIPELINE_ID))

            if all_alerts is None:
                all_alerts = tagged_df
            else:
                all_alerts = all_alerts.union(tagged_df)

        # --- LAYER 4: WRITE ALERTS TO POSTGRES ---
        if all_alerts:
            # Pack all dynamic columns into JSON string for the 'transaction_data' column
            output_df = all_alerts.select(
                col("pipeline_id"),
                col("severity"),
                col("rule_description"),
                current_timestamp().alias("alert_timestamp"),
                to_json(struct("*")).alias("transaction_data")
            )
            
            count = output_df.count()
            logger.warning(f"🚨 Detected {count} anomalies! Writing to DB...")
            
            # Create a temporary view to allow JDBC write
            output_df.createOrReplaceTempView("alerts_temp")            
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
        # Clear cache to free memory
        batch_df.unpersist()

def main():
    # 1. SETUP: Get R2 credentials from environment
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3_endpoint = os.getenv("S3_ENDPOINT")

    if not all([aws_access_key, aws_secret_key, s3_endpoint]):
        raise ValueError("Missing R2 credentials! Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_ENDPOINT")

    logger.info(f"Initializing Spark with R2 endpoint: {s3_endpoint}")

    # 2. SPARK SESSION
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

    # 3. ARGS PARSING
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-id", type=int, required=True, help="ID of the pipeline to run")
    args = parser.parse_known_args()[0]

    # IMPORTANT: We still need this global so process_batch can access it
    global PIPELINE_ID 
    PIPELINE_ID = args.pipeline_id

    # 4. FETCH SCHEMA (Static Configuration)
    # This remains in main() because schema changes require a job restart
    logger.info(f"Fetching configuration for Pipeline ID: {args.pipeline_id}")
    schema_json_str, kafka_topic = fetch_schema_from_db(spark, args.pipeline_id)

    schema_list = json.loads(schema_json_str)

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

    # ---------------------------------------------------------
    # DELETED: fetch_rules_from_db call (Moved to process_batch)
    # ---------------------------------------------------------

    # 5. KAFKA STREAM SETUP
    logger.info(f"Starting Kafka Stream (topic: {kafka_topic}, bootstrap: {KAFKA_BOOTSTRAP})...")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 6. PARSE JSON
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), dynamic_schema).alias("data")
    ).select("data.*")

    # Timestamp casting for Windowing/Watermarking if needed later
    if "timestamp" in parsed_df.columns:
        parsed_df = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp")))

    # 7. START STREAM
    logger.info("Starting structured streaming query with 10-second micro-batches...")
    
    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .start()

    logger.info(f"✅ Streaming query started (checkpoint: {CHECKPOINT_DIR})")
    query.awaitTermination()

if __name__ == "__main__":
    main()