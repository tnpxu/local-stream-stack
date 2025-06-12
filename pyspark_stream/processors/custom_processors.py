from pyspark.sql import DataFrame
from pyspark.sql.functions import upper, col, expr

from pyspark_stream.processors.base_processor import BaseProcessor
from typing import Optional, Any
import logging # For type hinting logger
from pyspark.sql import SparkSession # For type hinting SparkSession in constructor

class SimpleTransformProcessor(BaseProcessor):
    """
    A simple processor that converts the 'value' column of a DataFrame to uppercase,
    assuming 'value' contains string data. It also keeps the 'key' column.
    """

    def __init__(self, spark_session: SparkSession, logger: Optional[logging.Logger] = None, **kwargs: Any):
        super().__init__(spark_session, logger, **kwargs)
        # Example: Check for a specific configuration if needed
        # self.column_to_transform = kwargs.get("column_to_transform", "value")
        # self.logger.info(f"Configured to transform column: {self.column_to_transform}")

    def process(self, df: DataFrame) -> DataFrame:
        """
        Applies the uppercase transformation to the 'value' column.
        Assumes the DataFrame has 'key' and 'value' columns.

        Args:
            df (DataFrame): The input DataFrame. Expected to have at least
                            a 'value' column. A 'key' column is also preserved if present.

        Returns:
            DataFrame: The transformed DataFrame with 'value' in uppercase.
        """
        self.logger.info(f"{self.name()} processing DataFrame.")

        if "value" not in df.columns:
            self.logger.error("Input DataFrame does not have a 'value' column. Skipping transformation.")
            # Or raise ValueError("DataFrame must contain a 'value' column")
            return df

        # Ensure 'value' is string, then uppercase it.
        # Preserve 'key' if it exists, otherwise only transform 'value'.
        # The original job did: df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        # then selected key and uppercased value.
        # Here, we assume Kafka connector delivers key/value as string or binary.
        # If binary, it should be cast to string first.
        # The KafkaReader currently doesn't explicitly cast value, it returns what Spark Kafka source provides.
        # Typically, value from Kafka is binary, so casting is good practice.

        transformed_df = df.withColumn("value", upper(col("value").cast("string")))

        # Select only key and value if key exists, similar to original job's final select
        if "key" in df.columns:
            final_df = transformed_df.select(col("key"), col("value"))
            self.logger.info("Applied uppercase to 'value' and selected 'key', 'value'.")
        else:
            final_df = transformed_df.select(col("value")) # Should not happen if reading from Kafka
            self.logger.warning("Applied uppercase to 'value'. 'key' column was not present.")

        return final_df

if __name__ == '__main__':
    from pyspark.sql import SparkSession
    from pyspark_stream.utils.logger import get_logger as get_app_logger # Renamed to avoid conflict

    main_logger = get_app_logger("CustomProcessorsExample")
    main_logger.info("Starting CustomProcessors example...")

    spark = SparkSession.builder.appName("CustomProcessorsTest").master("local[*]").getOrCreate()

    # Example data for testing SimpleTransformProcessor
    data = [("key1", "hello world"), ("key2", "test message"), ("key3", "PySpark Stream")]
    schema = ["key", "value"]
    test_df = spark.createDataFrame(data, schema)
    test_df.show()

    main_logger.info("Instantiating SimpleTransformProcessor...")
    # Pass the actual Spark session for this local test
    simple_processor = SimpleTransformProcessor(spark_session=spark, logger=main_logger)

    main_logger.info(f"Processing test_df with {simple_processor.name()}...")
    transformed_df = simple_processor.process(test_df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")) # Ensure string types like Kafka would provide

    main_logger.info("Transformed DataFrame:")
    transformed_df.show()

    # Expected:
    # +----+----------------+
    # |key |value           |
    # +----+----------------+
    # |key1|HELLO WORLD     |
    # |key2|TEST MESSAGE    |
    # |key3|PYSPARK STREAM  |
    # +----+----------------+

    # Test with a DataFrame that doesn't have 'key' (should log warning but work)
    data_no_key = [("another message",), ("testing value only",)]
    schema_no_key = ["value"]
    test_df_no_key = spark.createDataFrame(data_no_key, schema_no_key)
    main_logger.info("Testing with DataFrame having only 'value' column:")
    test_df_no_key.show()
    transformed_no_key_df = simple_processor.process(test_df_no_key.selectExpr("CAST(value AS STRING) as value"))
    transformed_no_key_df.show()

    spark.stop()
    main_logger.info("CustomProcessors example finished.")
