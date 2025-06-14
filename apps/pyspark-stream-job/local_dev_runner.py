import os
from pyspark.sql import SparkSession
from sample_stream_job import run_simple_stream # Import from the new sample job file

def main():
    """
    Entry point for running the Spark Streaming job locally.
    This sets up a SparkSession configured to connect to the Spark-on-Kubernetes cluster.
    """
    print("Starting local development runner for sample job...")

    # This configuration tells Spark to run in client mode and connect to our Kubernetes cluster.
    # The Spark master is the Kubernetes API server.
    # The Spark driver (this script) runs locally, and executors run in pods.
    spark = (
        SparkSession.builder.appName("LocalStreamRunner")
        .master(f"k8s://https://kubernetes.default.svc")
        .config("spark.submit.deployMode", "client")
        .config("spark.kubernetes.container.image", "spark:3.5.0-python3")
        .config("spark.kubernetes.namespace", "spark-apps")
        .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config("spark.executor.instances", "2")
        .getOrCreate()
    )

    # The Kafka bootstrap server address when port-forwarded from Kubernetes.
    kafka_bootstrap_servers = "localhost:9094"

    try:
        # Run the simplified streaming job
        run_simple_stream(spark, kafka_bootstrap_servers)
    finally:
        print("Stopping Spark session.")
        spark.stop()


if __name__ == "__main__":
    main()
