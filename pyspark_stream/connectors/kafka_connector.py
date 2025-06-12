import os
import logging # <--- Add this import
from typing import Optional, Union, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery

from pyspark_stream.config.config_loader import load_config
from pyspark_stream.utils.logger import get_logger

class KafkaConnectorBase:
    """
    Base class for Kafka connectors, handling common configuration and logging.
    """
    def __init__(
        self,
        spark_session: SparkSession,
        kafka_config: Union[str, Dict],
        logger: Optional[logging.Logger] = None,
        config_key: str = 'kafka' # Key in the config dict where kafka specific settings reside
    ):
        self.spark = spark_session
        self.logger: logging.Logger = logger if logger else get_logger(self.__class__.__name__) # type hint logger explicitly

        if isinstance(kafka_config, str):
            self.logger.info(f"Loading Kafka configuration from path: {kafka_config}")
            # Assuming the main config file path is passed, and it contains a 'kafka' section
            full_config = load_config(kafka_config)
            self.kafka_config = full_config.get(config_key)
            if not self.kafka_config:
                msg = f"'{config_key}' section not found in configuration file: {kafka_config}"
                self.logger.error(msg)
                raise ValueError(msg)
        elif isinstance(kafka_config, dict):
            # If a dict is passed, it's assumed to be the kafka_config itself
            # or a larger config dict from which we need to extract the kafka_config
            if config_key in kafka_config: # e.g. kafka_config = {'kafka': {...}}
                self.kafka_config = kafka_config.get(config_key)
            else: # e.g. kafka_config = {...} (already the specific kafka settings)
                self.kafka_config = kafka_config
        else:
            msg = "kafka_config must be a path to a YAML file or a dictionary."
            self.logger.error(msg)
            raise TypeError(msg)

        if not self.kafka_config.get("bootstrap_servers"):
            msg = "Kafka bootstrap_servers is not defined in the configuration."
            self.logger.error(msg)
            raise ValueError(msg)

        self.common_options = {
            "kafka.bootstrap.servers": self.kafka_config["bootstrap_servers"]
        }
        self._configure_security()

    def _configure_security(self):
        """
        Configures Kafka security options based on the provided configuration.
        """
        security_config = self.kafka_config.get("security", {})
        protocol = security_config.get("protocol", "PLAINTEXT").upper()
        self.common_options["kafka.security.protocol"] = protocol

        self.logger.info(f"Configuring Kafka connection with security protocol: {protocol}")

        if protocol == "SASL_PLAINTEXT" or protocol == "SASL_SSL":
            sasl_mechanism = security_config.get("sasl_mechanism", "PLAIN").upper()
            self.common_options["kafka.sasl.mechanism"] = sasl_mechanism
            self.logger.info(f"Using SASL mechanism: {sasl_mechanism}")

            if sasl_mechanism in ["SCRAM-SHA-256", "SCRAM-SHA-512", "PLAIN"]:
                username = security_config.get("username")
                password_env_var = security_config.get("password_env_var")

                if not username or not password_env_var:
                    msg = f"Username and password_env_var must be set for {sasl_mechanism}."
                    self.logger.error(msg)
                    raise ValueError(msg)

                password = os.getenv(password_env_var)
                if not password:
                    msg = f"Environment variable {password_env_var} for Kafka password is not set."
                    self.logger.error(msg)
                    raise ValueError(msg)

                # For PLAIN, Jaas config is slightly different
                if sasl_mechanism == "PLAIN":
                    jaas_config = (
                        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                        f'username="{username}" password="{password}";'
                    )
                else: # SCRAM
                    jaas_config = (
                        f'org.apache.kafka.common.security.scram.ScramLoginModule required '
                        f'username="{username}" password="{password}";'
                    )
                self.common_options["kafka.sasl.jaas.config"] = jaas_config
                self.logger.info(f"JAAS config set for user '{username}' using {sasl_mechanism}.")
                # Do not log the actual JAAS config string as it contains the password.

            if protocol == "SASL_SSL":
                self._configure_ssl(security_config, protocol, "sasl.ssl.")

        elif protocol == "SSL":
            self._configure_ssl(security_config, protocol, "ssl.")

    def _configure_ssl(self, security_config: Dict, protocol: str, prefix: str):
        """Helper to configure SSL specific options."""
        truststore_loc = security_config.get("truststore_location")
        if truststore_loc:
            self.common_options[f"kafka.{prefix}truststore.location"] = truststore_loc
            self.logger.info(f"Using truststore: {truststore_loc}")
            tsp_env_var = security_config.get("truststore_password_env_var")
            if tsp_env_var:
                tsp = os.getenv(tsp_env_var)
                if not tsp:
                    self.logger.warning(f"Truststore password env var {tsp_env_var} is set in config but not in environment.")
                else:
                    self.common_options[f"kafka.{prefix}truststore.password"] = tsp
                    self.logger.info("Truststore password configured.")

        keystore_loc = security_config.get("keystore_location")
        if keystore_loc: # Typically for client auth
            self.common_options[f"kafka.{prefix}keystore.location"] = keystore_loc
            self.logger.info(f"Using keystore: {keystore_loc}")
            ksp_env_var = security_config.get("keystore_password_env_var")
            if ksp_env_var:
                ksp = os.getenv(ksp_env_var)
                if not ksp:
                    self.logger.warning(f"Keystore password env var {ksp_env_var} is set in config but not in environment.")
                else:
                    self.common_options[f"kafka.{prefix}keystore.password"] = ksp
                    # self.common_options[f"kafka.{prefix}key.password"] = ksp # Often key password is same as keystore
                    self.logger.info("Keystore password configured.")

        # For SSL/SASL_SSL, endpoint identification is important.
        # It defaults to 'https' if not specified in config.
        # An empty string in config means "disable endpoint identification".
        if protocol == "SSL" or protocol == "SASL_SSL":
            # Get value from config; if not present, endpoint_id_algo will be None
            endpoint_id_algo = security_config.get("ssl_endpoint_identification_algorithm")

            if endpoint_id_algo is None: # Not specified in config
                endpoint_id_algo = "https" # Default to 'https'
                self.logger.info(f"'ssl_endpoint_identification_algorithm' not specified, defaulting to '{endpoint_id_algo}'.")

            self.common_options[f"kafka.{prefix}endpoint.identification.algorithm"] = endpoint_id_algo
            self.logger.info(f"SSL endpoint identification algorithm set to: '{endpoint_id_algo}'")


class KafkaReader(KafkaConnectorBase):
    """
    Reads data from a Kafka topic using Spark Structured Streaming.
    """
    def __init__(
        self,
        spark_session: SparkSession,
        kafka_config: Union[str, Dict], # Path to main config YAML or dict of Kafka settings
        logger: Optional[logging.Logger] = None # Optional[logging.Logger]
    ):
        super().__init__(spark_session, kafka_config, logger, config_key='kafka')

        if not self.kafka_config.get("input_topic"):
            msg = "Kafka input_topic is not defined in the configuration."
            self.logger.error(msg)
            raise ValueError(msg)

    def read_stream(self, starting_offsets: str = "latest") -> DataFrame:
        """
        Sets up and returns a DataFrame for reading from a Kafka topic.

        Args:
            starting_offsets (str): From where to start reading data.
                                    Options: "earliest", "latest". Default is "latest".

        Returns:
            DataFrame: A Spark DataFrame configured to read from Kafka.
        """
        input_topic = self.kafka_config["input_topic"]
        options = self.common_options.copy()
        options["subscribe"] = input_topic
        options["startingOffsets"] = starting_offsets
        # Add any other reader-specific options from config if necessary
        # e.g., options["failOnDataLoss"] = "false"

        self.logger.info(f"Preparing to read from Kafka topic: {input_topic}")
        self.logger.info(f"Reader options (excluding sensitive JAAS): { {k:v for k,v in options.items() if 'jaas' not in k} }")

        return self.spark.readStream().format("kafka").options(**options).load()


class KafkaWriter(KafkaConnectorBase):
    """
    Writes data to a Kafka topic using Spark Structured Streaming.
    """
    def __init__(
        self,
        spark_session: SparkSession,
        kafka_config: Union[str, Dict], # Path to main config YAML or dict of Kafka settings
        logger: Optional[logging.Logger] = None # Optional[logging.Logger]
    ):
        super().__init__(spark_session, kafka_config, logger, config_key='kafka')

        if not self.kafka_config.get("output_topic"):
            msg = "Kafka output_topic is not defined in the configuration."
            self.logger.error(msg)
            raise ValueError(msg)

        # Checkpoint location is crucial for writers
        self.checkpoint_location = self.kafka_config.get("checkpoint_location_output")
        if not self.checkpoint_location:
            msg = "Kafka checkpoint_location_output is not defined for the writer."
            self.logger.error(msg)
            raise ValueError(msg)

    def write_stream(self, df: DataFrame, query_name: Optional[str] = None) -> StreamingQuery:
        """
        Writes a DataFrame to a Kafka topic.

        Args:
            df (DataFrame): The DataFrame to write.
            query_name (Optional[str]): Name for the streaming query.

        Returns:
            StreamingQuery: The started streaming query.
        """
        output_topic = self.kafka_config["output_topic"]
        options = self.common_options.copy()
        options["topic"] = output_topic

        # It's critical that the df has 'key' and 'value' columns,
        # or only 'value' if keys are not used.
        # Ensure 'value' is string or binary. If it's complex, it should be serialized (e.g., to JSON string).
        if "value" not in df.columns:
            msg = "DataFrame to write must have a 'value' column."
            self.logger.error(msg)
            raise ValueError(msg)

        # Typically, the value should be cast to string for Kafka, unless already binary
        # This depends on the upstream processing. For now, we assume it's correctly formatted.
        # from pyspark.sql.functions import col, to_json, struct
        # df_to_write = df.select(to_json(struct("*")).alias("value")) # Example: serialize all columns to JSON
        # Or if you have a specific column to be the value:
        # df_to_write = df.select(col("some_payload_column").cast("string").alias("value"))

        self.logger.info(f"Preparing to write to Kafka topic: {output_topic}")
        self.logger.info(f"Writer options (excluding sensitive JAAS): { {k:v for k,v in options.items() if 'jaas' not in k} }")

        stream_writer = df.writeStream().format("kafka").options(**options)

        if query_name:
            stream_writer.queryName(query_name)

        return stream_writer.option("checkpointLocation", self.checkpoint_location).start()

# Example Usage (for illustration, cannot be run directly without Spark and Kafka)
if __name__ == '__main__':
    # This example won't run in a typical non-Spark environment.
    # It's for conceptual validation of the class structure.
    # Ensure logging is imported if you use it directly like this,
    # but get_logger already returns a configured logger.
    main_example_logger = get_logger("KafkaConnectorExample") # Renamed to avoid conflict if logging was imported as logger
    main_example_logger.info("Starting KafkaConnector example (conceptual)")

    # Mock SparkSession for local testing (won't actually connect)
    class MockSparkSession:
        def __init__(self):
            self.sparkContext = None # Mock attribute
            self.conf = self # Mock conf attribute
        def readStream(self): # This is a method, so it should be callable
            main_example_logger.info("MockSparkSession.readStream() called")
            return MockDataFrameReader(main_example_logger)
        def streams(self): return self # Mock active streams
        def active(self): return [] # Mock active streams list

    class MockDataFrameReader:
        def __init__(self, logger_instance):
            self.logger = logger_instance
        def format(self, fmt):
            self.logger.info(f"MockDataFrameReader.format({fmt}) called")
            return self
        def options(self, **opts):
            self.logger.info(f"MockDataFrameReader.options({opts}) called")
            return self
        def load(self):
            self.logger.info("MockDataFrameReader.load() called")
            return MockDataFrame(self.logger)

    class MockDataFrame:
        def __init__(self, logger_instance):
            self.columns = ["value"] # Mock columns
            self.logger = logger_instance
        def writeStream(self): # This is a method, so it should be callable
            self.logger.info("MockDataFrame.writeStream() called")
            return MockStreamingDataWriter(self.logger)
        # Add other DF methods if needed for more complex tests e.g. select, alias, etc.

    class MockStreamingDataWriter:
        def __init__(self, logger_instance):
            self.logger = logger_instance
        def format(self, fmt):
            self.logger.info(f"MockStreamingDataWriter.format({fmt}) called")
            return self
        def options(self, **opts):
            self.logger.info(f"MockStreamingDataWriter.options({opts}) called")
            return self
        def option(self, key, value):
            self.logger.info(f"MockStreamingDataWriter.option({key}={value}) called")
            return self
        def queryName(self, name):
            self.logger.info(f"MockStreamingDataWriter.queryName({name}) called")
            return self
        def start(self):
            self.logger.info("MockStreamingDataWriter.start() called for writer query")
            return MockStreamingQuery(self.logger)

    class MockStreamingQuery:
        def __init__(self, logger_instance):
            self.logger = logger_instance
        def awaitTermination(self, timeout=None):
            self.logger.info(f"MockStreamingQuery.awaitTermination({timeout=}) called")
        def stop(self):
            self.logger.info("MockStreamingQuery.stop() called")


    # Set dummy environment variables for testing JAAS config construction
    os.environ["KAFKA_PASSWORD_TEST"] = "test_password"
    os.environ["KAFKA_OTHER_PASS_TEST"] = "other_test_password"

    sample_kafka_config_dict = {
        'kafka': {
            'bootstrap_servers': "dummy:9092",
            'input_topic': "input_test_topic",
            'output_topic': "output_test_topic",
            'checkpoint_location_output': "/tmp/dummy_checkpoint_output",
            'security': {
                'protocol': "SASL_PLAINTEXT",
                'sasl_mechanism': "SCRAM-SHA-512",
                'username': "test_user",
                'password_env_var': "KAFKA_PASSWORD_TEST"
            }
        }
    }

    sample_kafka_config_plain_dict = {
        'kafka': {
            'bootstrap_servers': "dummy:9092",
            'input_topic': "input_test_topic_plain",
            'output_topic': "output_test_topic_plain",
            'checkpoint_location_output': "/tmp/dummy_checkpoint_output_plain",
            'security': {
                'protocol': "SASL_PLAINTEXT",
                'sasl_mechanism': "PLAIN",
                'username': "plain_user",
                'password_env_var': "KAFKA_OTHER_PASS_TEST"
            }
        }
    }

    sample_kafka_config_ssl_dict = {
        'kafka': {
            'bootstrap_servers': "dummy:9093",
            'input_topic': "input_ssl_topic",
            'output_topic': "output_ssl_topic",
            'checkpoint_location_output': "/tmp/dummy_checkpoint_ssl",
            'security': {
                'protocol': "SASL_SSL",
                'sasl_mechanism': "SCRAM-SHA-512",
                'username': "ssl_user",
                'password_env_var': "KAFKA_PASSWORD_TEST",
                'truststore_location': "/fake/path/client.truststore.jks",
                'truststore_password_env_var': "TSP_ENV_VAR_NOT_SET", # Test warning
                'keystore_location': "/fake/path/client.keystore.jks",
                'keystore_password_env_var': "KSP_ENV_VAR_NOT_SET", # Test warning
                'ssl_endpoint_identification_algorithm': "" # disabled
            }
        }
    }

    sample_plaintext_config_dict = {
         'kafka': {
            'bootstrap_servers': "dummy:9094",
            'input_topic': "input_plaintext_topic",
            'output_topic': "output_plaintext_topic",
            'checkpoint_location_output': "/tmp/dummy_checkpoint_plaintext",
            # No 'security' key, should default to PLAINTEXT
        }
    }


    mock_spark = MockSparkSession()

    try:
        main_example_logger.info("\n--- Testing KafkaReader with SCRAM ---")
        # Pass the whole dict, connector will look for 'kafka' key
        kafka_reader = KafkaReader(spark_session=mock_spark, kafka_config=sample_kafka_config_dict, logger=main_example_logger)
        # Check common_options (sensitive JAAS config is not logged directly by the class)
        main_example_logger.info(f"Reader common options: { {k:v for k,v in kafka_reader.common_options.items() if 'jaas' not in k} }")
        assert "kafka.sasl.jaas.config" in kafka_reader.common_options
        assert 'password="test_password"' in kafka_reader.common_options["kafka.sasl.jaas.config"]
        assert 'username="test_user"' in kafka_reader.common_options["kafka.sasl.jaas.config"]
        mock_df_reader = kafka_reader.read_stream()

        main_example_logger.info("\n--- Testing KafkaWriter with SCRAM ---")
         # Pass only the kafka specific part of the config
        kafka_writer = KafkaWriter(spark_session=mock_spark, kafka_config=sample_kafka_config_dict['kafka'], logger=main_example_logger)
        main_example_logger.info(f"Writer common options: { {k:v for k,v in kafka_writer.common_options.items() if 'jaas' not in k} }")
        assert "kafka.sasl.jaas.config" in kafka_writer.common_options
        mock_query = kafka_writer.write_stream(MockDataFrame(main_example_logger), query_name="TestQuery")

        main_example_logger.info("\n--- Testing KafkaReader with PLAIN ---")
        kafka_reader_plain = KafkaReader(spark_session=mock_spark, kafka_config=sample_kafka_config_plain_dict, logger=main_example_logger)
        main_example_logger.info(f"Reader PLAIN common options: { {k:v for k,v in kafka_reader_plain.common_options.items() if 'jaas' not in k} }")
        assert "kafka.sasl.jaas.config" in kafka_reader_plain.common_options
        assert 'password="other_test_password"' in kafka_reader_plain.common_options["kafka.sasl.jaas.config"]
        assert 'username="plain_user"' in kafka_reader_plain.common_options["kafka.sasl.jaas.config"]
        assert "PlainLoginModule" in kafka_reader_plain.common_options["kafka.sasl.jaas.config"]
        mock_df_reader_plain = kafka_reader_plain.read_stream()

        main_example_logger.info("\n--- Testing KafkaReader with SASL_SSL ---")
        # Example of using a config file path (will be mocked by load_config)
        # For this to work, we'd need to mock load_config or create a dummy file
        # For now, directly pass the dict for SSL test
        kafka_reader_ssl = KafkaReader(spark_session=mock_spark, kafka_config=sample_kafka_config_ssl_dict, logger=main_example_logger)
        main_example_logger.info(f"Reader SASL_SSL common options: { {k:v for k,v in kafka_reader_ssl.common_options.items() if 'jaas' not in k} }")
        assert kafka_reader_ssl.common_options.get("kafka.sasl.ssl.truststore.location") == "/fake/path/client.truststore.jks"
        assert kafka_reader_ssl.common_options.get("kafka.sasl.ssl.keystore.location") == "/fake/path/client.keystore.jks"
        assert kafka_reader_ssl.common_options.get("kafka.sasl.ssl.endpoint.identification.algorithm") == ""
        # Check that password env vars not set result in absence of password options (logged as warning by class)
        assert "kafka.sasl.ssl.truststore.password" not in kafka_reader_ssl.common_options
        assert "kafka.sasl.ssl.keystore.password" not in kafka_reader_ssl.common_options
        mock_df_reader_ssl = kafka_reader_ssl.read_stream()

        main_example_logger.info("\n--- Testing KafkaReader with PLAINTEXT (no security block) ---")
        kafka_reader_plaintext = KafkaReader(spark_session=mock_spark, kafka_config=sample_plaintext_config_dict, logger=main_example_logger)
        main_example_logger.info(f"Reader PLAINTEXT common options: {kafka_reader_plaintext.common_options}")
        assert kafka_reader_plaintext.common_options.get("kafka.security.protocol") == "PLAINTEXT"
        assert "kafka.sasl.mechanism" not in kafka_reader_plaintext.common_options # No SASL for PLAINTEXT by default
        mock_df_reader_plaintext = kafka_reader_plaintext.read_stream()

        main_example_logger.info("\nKafkaConnector example finished successfully (conceptual).")

    except ValueError as ve:
        main_example_logger.error(f"ValueError in KafkaConnector example: {ve}")
    except TypeError as te:
        main_example_logger.error(f"TypeError in KafkaConnector example: {te}")
    except Exception as e:
        main_example_logger.error(f"Unexpected exception in KafkaConnector example: {e}", exc_info=True)

    # Clean up env vars
    del os.environ["KAFKA_PASSWORD_TEST"]
    del os.environ["KAFKA_OTHER_PASS_TEST"]
# The import logging was added at the top. This comment is now obsolete.
# import logging # Add this at the top with other imports if not already there for KafkaConnectorBase logger type hint
# (already imported by logger.py but good for explicitness if this file was standalone)
