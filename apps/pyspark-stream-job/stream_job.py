import os
from pyspark.sql import SparkSession
from pyspark_stream.pipeline.streaming_pipeline import StreamingPipeline
from pyspark_stream.utils.logger import get_logger # Optional: for job-specific logging

# It's good practice to have a logger for the main job script as well.
logger = get_logger("StreamJobRunner")

if __name__ == "__main__":
    logger.info("Starting PySpark Streaming Job using the framework...")

    # Check for the required environment variable for Kafka password
    # The pipeline config references KAFKA_ADMIN_PASSWORD.
    # The KafkaConnector will fetch it from the environment.
    # This check is just to ensure it's set before Spark even starts,
    # failing fast if it's missing.
    if not os.environ.get("KAFKA_ADMIN_PASSWORD"):
        logger.error("CRITICAL: KAFKA_ADMIN_PASSWORD environment variable not set!")
        # In a real K8s/container setup, the entrypoint script or pod spec should ensure this.
        # For local testing, it needs to be set in the shell.
        raise ValueError("KAFKA_ADMIN_PASSWORD environment variable not set!")
    else:
        logger.info("KAFKA_ADMIN_PASSWORD environment variable is set.")


    spark = None # Initialize spark to None for finally block
    pipeline_instance = None # Initialize pipeline_instance for finally block
    try:
        spark = SparkSession.builder.appName("StreamJobRefactoredWithFramework").getOrCreate()

        # Optional: Set log level (can also be configured in Spark submit or log4j.properties)
        # spark.sparkContext.setLogLevel("WARN") # The pipeline logger will handle its own verbosity.

        logger.info("Spark Session created.")

        # Define the path to the pipeline configuration file for this specific job
        # This path should be accessible within the execution environment (e.g., container).
        # Assuming the script is run from the root of the repository, or /app in container.
        pipeline_config_path = "configs/stream_job_pipeline_config.yaml"
        logger.info(f"Using pipeline configuration from: {pipeline_config_path}")

        # Instantiate and run the pipeline
        pipeline_instance = StreamingPipeline(spark_session=spark, pipeline_config_path=pipeline_config_path)

        logger.info("Building the pipeline...")
        pipeline_instance.build()

        logger.info("Running the pipeline...")
        # The run method by default will block with awaitTermination()
        # unless configured otherwise or if run(await_termination=False) is called.
        pipeline_instance.run()

        logger.info("Pipeline execution finished or was terminated.")

    except FileNotFoundError as fnf_error:
        logger.error(f"Pipeline configuration file not found: {fnf_error}")
        # This might indicate an issue with deployment or incorrect path.
    except ValueError as val_error: # Catch ValueErrors from pipeline setup (e.g. missing config keys)
        logger.error(f"Configuration error in pipeline: {val_error}")
    except Exception as e:
        logger.error(f"An unexpected error occurred in StreamJob: {e}", exc_info=True)
    finally:
        if spark:
            logger.info("Stopping Spark Session.")
            # pipeline.stop() # This could be called here if run was non-blocking
            # For now, if run() blocks, it will only reach here after termination.
            # If run() itself had an error, stopping active queries might be useful.
            # The pipeline.stop() is a basic version, a more robust one might be needed.
            try:
                if pipeline_instance and hasattr(pipeline_instance, 'stop'): # Ensure pipeline was initialized
                    pipeline_instance.stop() # Attempt to stop queries if pipeline object exists
            except Exception as stop_e:
                logger.error(f"Exception during pipeline stop: {stop_e}", exc_info=True)
            finally:
                spark.stop()
                logger.info("Spark Session stopped.")
        else:
            logger.info("Spark Session was not initialized or already stopped.")

    logger.info("StreamJobRunner finished.")
