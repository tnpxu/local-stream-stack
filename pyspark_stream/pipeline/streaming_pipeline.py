import importlib
from typing import Optional, List, Dict, Any

from pyspark.sql import SparkSession, DataFrame

from pyspark_stream.processors.base_processor import BaseProcessor
from pyspark_stream.connectors.kafka_connector import KafkaReader, KafkaWriter
from pyspark_stream.utils.logger import get_logger
from pyspark_stream.config.config_loader import load_config
# from pyspark_stream.models.schema_manager import get_schema # If schemas were applied here
import logging # For type hinting logger

class StreamingPipeline:
    """
    Orchestrates a Spark Structured Streaming pipeline from a source,
    through a series of processors, to a sink, based on a configuration file.
    """

    def __init__(self, spark_session: SparkSession, pipeline_config_path: str):
        """
        Initializes the StreamingPipeline.

        Args:
            spark_session (SparkSession): The Spark session.
            pipeline_config_path (str): Path to the YAML configuration file for the pipeline.
        """
        self.spark = spark_session
        self.pipeline_config_path = pipeline_config_path

        try:
            self.pipeline_config = load_config(pipeline_config_path)
        except FileNotFoundError:
            # This error is already logged by load_config, but we might want to re-raise or handle
            # Here, we'll let it propagate if load_config raises it.
            # If load_config returns None or empty, handle that.
            self.logger = get_logger("StreamingPipeline_init_error") # Temp logger
            self.logger.error(f"Pipeline configuration file not found at: {pipeline_config_path}")
            raise
        except Exception as e:
            self.logger = get_logger("StreamingPipeline_init_error")
            self.logger.error(f"Error loading pipeline configuration from {pipeline_config_path}: {e}")
            raise

        app_name = self.pipeline_config.get("app_name", "StreamingPipeline")
        self.logger = get_logger(app_name) # Main logger for the pipeline instance

        self.reader: Optional[KafkaReader] = None # Assuming KafkaReader for now
        self.writer: Optional[KafkaWriter] = None # Assuming KafkaWriter for now
        self.processors: List[BaseProcessor] = []

        self.logger.info(f"StreamingPipeline '{app_name}' initialized with config: {pipeline_config_path}")

    def _create_reader(self, reader_config: Dict[str, Any]):
        """
        Creates a reader component based on the provided configuration.
        """
        reader_type = reader_config.get("type")
        reader_actual_config = reader_config.get("config")

        if not reader_type or not reader_actual_config:
            self.logger.error("Reader 'type' or 'config' not specified in pipeline configuration.")
            raise ValueError("Reader 'type' or 'config' not specified.")

        self.logger.info(f"Creating reader of type: {reader_type}")
        if reader_type.lower() == "kafka":
            # KafkaReader expects the Kafka-specific config directly
            self.reader = KafkaReader(spark_session=self.spark, kafka_config=reader_actual_config, logger=self.logger)
            self.logger.info("KafkaReader created successfully.")
        else:
            self.logger.error(f"Unsupported reader type: {reader_type}")
            raise ValueError(f"Unsupported reader type: {reader_type}")

    def _create_writer(self, writer_config: Dict[str, Any]):
        """
        Creates a writer component based on the provided configuration.
        """
        writer_type = writer_config.get("type")
        writer_actual_config = writer_config.get("config")

        if not writer_type or not writer_actual_config:
            self.logger.error("Writer 'type' or 'config' not specified in pipeline configuration.")
            raise ValueError("Writer 'type' or 'config' not specified.")

        self.logger.info(f"Creating writer of type: {writer_type}")
        if writer_type.lower() == "kafka":
            # KafkaWriter expects the Kafka-specific config directly
            self.writer = KafkaWriter(spark_session=self.spark, kafka_config=writer_actual_config, logger=self.logger)
            self.logger.info("KafkaWriter created successfully.")
        else:
            self.logger.error(f"Unsupported writer type: {writer_type}")
            raise ValueError(f"Unsupported writer type: {writer_type}")

    def add_processor(self, processor: BaseProcessor):
        """
        Adds a pre-initialized processor instance to the pipeline.
        """
        if not isinstance(processor, BaseProcessor):
            self.logger.error(f"Attempted to add an invalid processor. Must be instance of BaseProcessor. Got: {type(processor)}")
            raise TypeError("Processor must be an instance of BaseProcessor.")
        self.processors.append(processor)
        self.logger.info(f"Processor '{processor.name()}' added to the pipeline.")

    def _create_processors(self, processor_configs: List[Dict[str, Any]]):
        """
        Dynamically creates and adds processor components based on their configurations.
        """
        if not processor_configs:
            self.logger.info("No processors configured for the pipeline.")
            return

        for i, proc_config in enumerate(processor_configs):
            class_path = proc_config.get("class")
            params = proc_config.get("params", {})
            processor_name_from_config = proc_config.get("name", f"Processor_{i+1}") # For logging

            if not class_path:
                self.logger.error(f"Processor class not specified in config for processor {processor_name_from_config}.")
                raise ValueError(f"Processor class not specified for {processor_name_from_config}")

            self.logger.info(f"Creating processor '{processor_name_from_config}' from class: {class_path}")
            try:
                module_name, class_name = class_path.rsplit(".", 1)
                ProcessorClass = getattr(importlib.import_module(module_name), class_name)

                # Pass spark_session, a child logger, and other params
                # Create a child logger for the processor for better context
                processor_logger = get_logger(f"{self.logger.name}.{ProcessorClass.__name__}_{i}")

                processor_instance = ProcessorClass(
                    spark_session=self.spark,
                    logger=processor_logger,
                    **params
                )
                self.add_processor(processor_instance)
            except ImportError as e:
                self.logger.error(f"Error importing module for processor '{class_path}': {e}")
                raise
            except AttributeError as e:
                self.logger.error(f"Error getting class '{class_name}' from module '{module_name}': {e}")
                raise
            except Exception as e:
                self.logger.error(f"Error instantiating processor '{class_path}' with params {params}: {e}")
                raise

    def build(self):
        """
        Builds the pipeline by creating reader, processors, and writer
        based on the loaded configuration.
        """
        self.logger.info("Building streaming pipeline...")

        source_config = self.pipeline_config.get("source")
        if not source_config:
            self.logger.error("Pipeline 'source' configuration is missing.")
            raise ValueError("Pipeline 'source' configuration is missing.")
        self._create_reader(source_config)

        processor_configs = self.pipeline_config.get("processors", []) # Default to empty list
        self._create_processors(processor_configs)

        sink_config = self.pipeline_config.get("sink")
        if not sink_config:
            self.logger.error("Pipeline 'sink' configuration is missing.")
            raise ValueError("Pipeline 'sink' configuration is missing.")
        self._create_writer(sink_config)

        self.logger.info("Streaming pipeline built successfully.")
        self.logger.info(f"Reader: {self.reader.__class__.__name__ if self.reader else 'None'}")
        self.logger.info(f"Processors: {[p.name() for p in self.processors]}")
        self.logger.info(f"Writer: {self.writer.__class__.__name__ if self.writer else 'None'}")


    def run(self, await_termination: bool = True):
        """
        Runs the streaming pipeline.

        Reads from the source, applies transformations through processors,
        and writes to the sink.

        Args:
            await_termination (bool): If True, the call will block until the stream
                                      is terminated. Default is True.
        """
        if not self.reader:
            self.logger.error("Reader is not initialized. Cannot run pipeline. Did you call build()?")
            return
        if not self.writer: # Writer might be optional if only processing and no sink, but for now assume required
            self.logger.error("Writer is not initialized. Cannot run pipeline. Did you call build()?")
            return

        self.logger.info("Starting streaming pipeline run...")
        try:
            df: DataFrame = self.reader.read_stream()

            for processor in self.processors:
                self.logger.info(f"Applying processor: {processor.name()}")
                df = processor.process(df)
                if df is None: # Processor might intentionally stop pipeline
                    self.logger.warning(f"Processor {processor.name()} returned None. Stopping pipeline.")
                    return

            self.logger.info("All processors applied. Writing to sink...")
            query = self.writer.write_stream(df)

            if await_termination:
                self.logger.info(f"Streaming query '{query.name if query.name else query.id}' started. Awaiting termination...")
                query.awaitTermination()
                self.logger.info("Streaming query terminated.")
            else:
                self.logger.info(f"Streaming query '{query.name if query.name else query.id}' started in background.")
                # The user of this class would be responsible for managing the query object (e.g. query.stop())
                return query # Return the query object if not awaiting

        except Exception as e:
            self.logger.error(f"An error occurred during pipeline execution: {e}", exc_info=True)
            # Consider stopping the Spark session or specific queries if needed
            # self.spark.stop() # Or manage query stopping more gracefully
            raise
        return None # If await_termination is true and finishes or if error before query start

    def stop(self):
        """
        Stops all active streaming queries managed by this Spark session.
        This is a basic implementation. More granular control might be needed.
        """
        self.logger.info("Attempting to stop active streaming queries...")
        try:
            # spark.streams.active is a property that returns a list, so call it if it's a method in mock
            active_queries = self.spark.streams.active
            if callable(active_queries): # Check if it's the method from mock
                active_queries = active_queries()

            for query in active_queries:
                self.logger.info(f"Stopping query: Name='{query.name}', ID='{query.id}'")
                query.stop()
            self.logger.info("All active queries signaled to stop.")
        except Exception as e:
            self.logger.error(f"Error while stopping queries: {e}", exc_info=True)


# --- Example Usage ---
if __name__ == '__main__':
    # This example requires KAFKA_PIPELINE_PASSWORD to be set in the environment
    # For local testing, we can set it temporarily if it's not present.
    import os
    if "KAFKA_PIPELINE_PASSWORD" not in os.environ:
        os.environ["KAFKA_PIPELINE_PASSWORD"] = "test_pipeline_password"
        print("Temporarily set KAFKA_PIPELINE_PASSWORD for example run.")


    main_logger = get_logger("StreamingPipelineExample")
    main_logger.info("Starting StreamingPipeline example...")

    # Mock SparkSession for the example
    class MockSparkSession:
        def __init__(self, app_name="MockSpark"):
            self._app_name = app_name
            self.sparkContext = "MockSparkContext"
            self.streams = self # Mock streams attribute
            main_logger.info(f"MockSparkSession '{self._app_name}' created.")

        @property
        def conf(self):
            class MockConf:
                def get(self, key, default=None): return default
                def set(self, key, value): pass
            return MockConf()

        def active(self): # Mock for spark.streams.active
            main_logger.info("MockSparkSession.streams.active called, returning empty list.")
            return []


    mock_spark_session = MockSparkSession()

    # Path to the sample pipeline config created in the previous step
    # Assuming it's in ../configs/pipeline_config.yaml relative to this file if run directly
    # For the tool environment, /app/configs/pipeline_config.yaml is correct
    pipeline_config_file = "configs/pipeline_config.yaml"

    try:
        main_logger.info(f"Creating StreamingPipeline with config: {pipeline_config_file}")
        pipeline = StreamingPipeline(spark_session=mock_spark_session, pipeline_config_path=pipeline_config_file)

        main_logger.info("Building the pipeline...")
        pipeline.build()

        # The run() method will use mocked KafkaReader/Writer which won't actually connect
        # but will log their actions. The DataFrame will also be a mock.
        main_logger.info("Running the pipeline (conceptual)...")

        # Mock the behavior of KafkaReader and KafkaWriter for the test run
        # This is a bit complex for a simple __main__ but shows the flow
        class MockStreamingQuery:
            def __init__(self, name="mock_query"):
                self.name = name
                self.id = "mock_id_123"
                main_logger.info(f"MockStreamingQuery '{self.name}' (id={self.id}) created.")
            def awaitTermination(self):
                main_logger.info(f"MockStreamingQuery '{self.name}' awaitTermination() called. Simulating completion.")
            def stop(self):
                main_logger.info(f"MockStreamingQuery '{self.name}' stop() called.")

        class MockDataFrame:
            def __init__(self, spark_session, id_val="mock_df"):
                self.sparkSession = spark_session # Some operations might need this
                self._id = id_val
                main_logger.info(f"MockDataFrame(id='{self._id}') created.")
            def __repr__(self): return f"MockDataFrame(id='{self._id}')"

        # Replace actual reader/writer process methods for testing without real Spark execution
        original_reader_read_stream = None
        if pipeline.reader:
            original_reader_read_stream = pipeline.reader.read_stream
            def mock_read_stream():
                main_logger.info("Mocked KafkaReader.read_stream() called.")
                return MockDataFrame(mock_spark_session, "df_from_reader")
            pipeline.reader.read_stream = mock_read_stream

        original_writer_write_stream = None
        if pipeline.writer:
            original_writer_write_stream = pipeline.writer.write_stream
            def mock_write_stream(df_to_write):
                main_logger.info(f"Mocked KafkaWriter.write_stream() called with {df_to_write}.")
                return MockStreamingQuery(name=pipeline.writer.kafka_config.get("query_name", "MyPipelineQueryMock"))
            pipeline.writer.write_stream = mock_write_stream

        pipeline.run(await_termination=True) # Set to False to test non-blocking if needed

        main_logger.info("Pipeline run finished (conceptual).")

        # Test stop
        main_logger.info("Testing pipeline.stop()...")
        pipeline.stop() # Will call spark.streams.active which is mocked

    except FileNotFoundError:
        main_logger.error(f"CRITICAL: Could not find pipeline configuration: {pipeline_config_file}. "
                          "Ensure the path is correct relative to where the script is run.")
    except ValueError as ve:
        main_logger.error(f"ValueError during pipeline setup or build: {ve}")
    except Exception as e:
        main_logger.error(f"An unexpected error occurred in the example: {e}", exc_info=True)
    finally:
        if "KAFKA_PIPELINE_PASSWORD" in os.environ and os.environ["KAFKA_PIPELINE_PASSWORD"] == "test_pipeline_password":
            del os.environ["KAFKA_PIPELINE_PASSWORD"]
            print("Cleaned up temporary KAFKA_PIPELINE_PASSWORD.")

    main_logger.info("StreamingPipeline example finished.")
