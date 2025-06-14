import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_simple_stream(spark: SparkSession, kafka_bootstrap_servers: str):
    """
    A simple Spark Structured Streaming job that consumes from a Kafka topic
    and prints the output to the console.

    Args:
        spark: The SparkSession object.
        kafka_bootstrap_servers: The comma-separated list of Kafka bootstrap servers.
    """
    kafka_topic = "my-topic"

    print(f"Starting stream from Kafka topic: {kafka_topic}")
    print(f"Using Kafka bootstrap servers: {kafka_bootstrap_servers}")

    try:
        # Read from the Kafka topic
        # The 'value' column will contain the message content as binary data
        kafka_stream_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "earliest")  # Read from the beginning of the topic
            .load()
        )

        # The key and value are binary by default, let's cast the value to a string
        # so we can see it in the console.
        string_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")

        # Write the stream to the console
        query = (
            string_df.writeStream.outputMode("append")
            .format("console")
            .option("truncate", "false") # Show the full content of the message
            .start()
        )

        print("Streaming query started. Waiting for termination...")
        query.awaitTermination()

    except Exception as e:
        print(f"An error occurred during the streaming job: {e}")
        # In a real application, you'd have more robust error handling.


if __name__ == "__main__":
    # This block allows the script to be run directly with spark-submit
    # It demonstrates how to set up the SparkSession for cluster execution.
    
    # These would typically be passed in via spark-submit arguments or environment variables
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")

    # When submitting to a Spark cluster (not in client mode like our runner)
    # the SparkSession is built like this.
    spark_session = (
        SparkSession.builder.appName("SimpleStreamJob")
        # Add any other necessary Spark configurations here
        .getOrCreate()
    )

    run_simple_stream(spark_session, KAFKA_BOOTSTRAP_SERVERS)
