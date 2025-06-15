# --- Use findspark to locate and initialize the local Spark installation ---
# This must be the very first import. It finds the Spark installation from your
# SPARK_HOME environment variable and configures the Python environment to use it.
import findspark
findspark.init()

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def verify_kafka_connection(spark: SparkSession, kafka_bootstrap_servers: str, kafka_topic: str, jaas_config: str):
    """
    Performs a batch query to Kafka to verify the connection and authentication.
    Returns True if successful, False otherwise.
    """
    print("--- Verifying Kafka connection and authentication ---")
    try:
        # This batch query forces a connection and will fail if auth is incorrect.
        test_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", kafka_topic)
            .option("kafka.security.protocol", "SASL_PLAINTEXT")
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
            # Set the JAAS config directly as a Kafka property
            .option("kafka.sasl.jaas.config", jaas_config)
            .option("kafka.request.timeout.ms", "10000") # 10 seconds
            .load()
        )
        
        # This action triggers the query.
        test_df.select("topic").take(1)
        
        print("--- Kafka connection and authentication successful! ---")
        return True

    except Exception as e:
        print("\n--- FATAL: Failed to connect to Kafka. ---")
        print("Please check:")
        print("  1. The Kafka port-forward is running (`kubectl port-forward ...`).")
        print("  2. The KAFKA_PASSWORD environment variable is set correctly.")
        print("  3. The topic name is correct and the topic exists.")
        print(f"\nError details: {e}\n")
        return False


def run_simple_stream(spark: SparkSession, kafka_bootstrap_servers: str, jaas_config: str):
    """
    A simple Spark Structured Streaming job that consumes from a Kafka topic
    and prints the output to the console.
    """
    kafka_topic = "my-first-topic"

    # --- Run the pre-flight check ---
    if not verify_kafka_connection(spark, kafka_bootstrap_servers, kafka_topic, jaas_config):
        print("--- Halting application due to Kafka connection failure. ---")
        return

    print(f"\n--- Starting stream from Kafka topic: {kafka_topic} ---")
    try:
        kafka_stream_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "earliest")
            .option("kafka.security.protocol", "SASL_PLAINTEXT")
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
            # Set the JAAS config directly as a Kafka property
            .option("kafka.sasl.jaas.config", jaas_config)
            .load()
        )

        string_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")

        query = (
            string_df.writeStream.outputMode("append")
            .format("console")
            .option("truncate", "false")
            .start()
        )

        print("Streaming query started. Waiting for termination...")
        query.awaitTermination()

    except Exception as e:
        print(f"An error occurred during the streaming job: {e}")


if __name__ == "__main__":
    # This block now contains the full logic for local development execution.
    print("Starting local development job...")

    # --- Get Kafka Password ---
    kafka_password = os.getenv("KAFKA_PASSWORD")
    if not kafka_password:
        print("ERROR: KAFKA_PASSWORD environment variable not set.")
        print("Please set it before running. Example:")
        print("  export KAFKA_PASSWORD=$(kubectl get secret admin-user -n kafka -o jsonpath='{.data.password}' | base64 --decode)")
        exit() # Exit if password is not set
    
    kafka_user = "admin-user"
    
    # Simple JAAS config string without the KafkaClient wrapper.
    jaas_config = f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{kafka_user}" password="{kafka_password}";'
    
    # Create a local SparkSession.
    spark = (
        SparkSession.builder.appName("LocalStreamRunner")
        .master("local[*]")
        # --- FIX for networking/hostname lookup issues on some machines ---
        .config("spark.driver.host", "127.0.0.1")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        # We no longer set the JAAS config globally here.
        .getOrCreate()
    )

    # The Kafka bootstrap server address when port-forwarded from Kubernetes.
    kafka_bootstrap_servers = "localhost:9094"

    try:
        # Run the simplified streaming job
        run_simple_stream(spark, kafka_bootstrap_servers, jaas_config)
    finally:
        print("Stopping Spark session.")
        spark.stop()
