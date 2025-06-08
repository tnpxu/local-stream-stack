import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

# --- Kafka Connection Details ---
# These are the internal addresses and topics for communication within Kubernetes.
KAFKA_BOOTSTRAP_SERVERS = "my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
INPUT_TOPIC_NAME = "input_topic"
OUTPUT_TOPIC_NAME = "output_topic"

# --- Authentication ---
# We get the password from an environment variable for security.
# The Spark Operator will mount the Kubernetes secret into our pod as an environment variable.
KAFKA_PASSWORD = os.environ.get("KAFKA_ADMIN_PASSWORD")
if not KAFKA_PASSWORD:
    raise ValueError("KAFKA_ADMIN_PASSWORD environment variable not set!")

# This JAAS config tells the Kafka client how to authenticate.
# It uses the username 'admin-user' and the password from the environment variable.
JAAS_CONFIG = (
    "org.apache.kafka.common.security.scram.ScramLoginModule required "
    f'username="admin-user" password="{KAFKA_PASSWORD}";'
)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PySparkKafkaStreamJob").getOrCreate()
    
    # Set log level to WARN to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    print("Spark Session created. Reading from Kafka input_topic...")

    # --- Read Stream from Kafka ---
    kafka_input_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", INPUT_TOPIC_NAME)
        # Authentication options
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.sasl.jaas.config", JAAS_CONFIG)
        .load()
    )

    # --- Define Schema and Transform Data ---
    # We cast the message value from binary to a string, then convert it to uppercase.
    transformed_df = kafka_input_df.select(
        col("key"), # Keep the original message key
        upper(col("value").cast("string")).alias("value") # Transform value to uppercase
    )

    print("Stream transformation defined. Writing back to Kafka output_topic...")

    # --- Write Transformed Stream back to Kafka ---
    query = (
        transformed_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", OUTPUT_TOPIC_NAME)
        # Checkpoint location for stream state recovery
        .option("checkpointLocation", "/tmp/spark-checkpoints")
        # Authentication options for the writer
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.sasl.jaas.config", JAAS_CONFIG)
        .start()
    )

    print("Streaming query started. This job will run indefinitely.")
    # awaitTermination() makes the job wait for the stream to be terminated
    query.awaitTermination()