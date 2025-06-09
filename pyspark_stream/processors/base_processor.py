from abc import ABC, abstractmethod
from typing import Optional, Any, Dict

from pyspark.sql import SparkSession, DataFrame
# It's good practice to ensure pyspark.sql.types are available if processors might need them,
# though not strictly required by BaseProcessor itself.
# from pyspark.sql.types import StructType

from pyspark_stream.utils.logger import get_logger
import logging # For type hinting logger

class BaseProcessor(ABC):
    """
    Abstract base class for data processors.
    Subclasses must implement the `process` method.
    """

    def __init__(self, spark_session: SparkSession, logger: Optional[logging.Logger] = None, **kwargs: Any):
        """
        Initializes the BaseProcessor.

        Args:
            spark_session (SparkSession): The Spark session to be used by the processor.
            logger (Optional[logging.Logger]): An external logger instance. If None,
                                               a new logger is created.
            **kwargs: Additional keyword arguments that will be stored as instance attributes.
                      This allows passing arbitrary configurations to processors.
        """
        self.spark = spark_session
        self.logger = logger if logger else get_logger(self.__class__.__name__)

        # Store additional configurations as instance attributes
        for key, value in kwargs.items():
            setattr(self, key, value)
            self.logger.info(f"Set processor config: {key} = {value}")

        self.logger.info(f"Initialized {self.name()} processor.")

    @abstractmethod
    def process(self, df: DataFrame) -> DataFrame:
        """
        Processes an input DataFrame and returns a transformed DataFrame.
        This method must be implemented by subclasses.

        Args:
            df (DataFrame): The input DataFrame.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        pass

    def name(self) -> str:
        """
        Returns the class name of the processor.

        Returns:
            str: The class name.
        """
        return self.__class__.__name__

# --- Example Usage ---
class NoOpProcessor(BaseProcessor):
    """
    A simple processor that does nothing but return the input DataFrame.
    It demonstrates how to inherit from BaseProcessor.
    """
    def __init__(self, spark_session: SparkSession, logger: Optional[logging.Logger] = None, **kwargs: Any):
        # You can call super().__init__ first
        super().__init__(spark_session, logger, **kwargs)
        # Or do other initializations specific to NoOpProcessor if needed
        # For example, log a specific message for NoOpProcessor
        self.logger.info(f"{self.name()} specific initialization completed.")


    def process(self, df: DataFrame) -> DataFrame:
        """
        Returns the input DataFrame without any transformation.
        """
        self.logger.info(f"{self.name()} received a DataFrame, returning it as is.")
        # Example of accessing a config passed via kwargs
        if hasattr(self, 'custom_message'):
            self.logger.info(f"Custom message from config: {self.custom_message}")
        return df

if __name__ == '__main__':
    main_logger = get_logger("BaseProcessorExample")
    main_logger.info("Starting BaseProcessor example...")

    # Mock SparkSession for the example
    class MockSparkSession:
        def __init__(self):
            self.sparkContext = "MockSparkContext" # Example attribute
            main_logger.info("MockSparkSession created for example.")
        # Add any other methods or attributes your BaseProcessor or its children might expect
        # For example, if a processor uses self.spark.conf.get(...)
        @property
        def conf(self):
            class MockConf:
                def get(self, key, default=None):
                    main_logger.info(f"MockSparkSession.conf.get('{key}', default='{default}') called.")
                    return default
            return MockConf()

    mock_spark = MockSparkSession()

    # Mock DataFrame for the example
    class MockDataFrame:
        def __init__(self, id_val: str = "mock_df"):
            self._id = id_val # To help distinguish if it's the same instance
            main_logger.info(f"MockDataFrame(id='{self._id}') created for example.")

        def __repr__(self):
            return f"MockDataFrame(id='{self._id}')"

    # 1. Test BaseProcessor instantiation (cannot be done directly as it's ABC)
    main_logger.info("Testing direct instantiation of BaseProcessor (should fail or be prevented)...")
    try:
        # base_proc = BaseProcessor(mock_spark) # This would fail if Python enforces ABC at instantiation
        main_logger.info("Direct instantiation of ABC is typically an error if attempted.")
    except TypeError as e:
        main_logger.info(f"Correctly caught TypeError for ABC instantiation: {e}")

    # 2. Test NoOpProcessor
    main_logger.info("\n--- Testing NoOpProcessor ---")
    noop_processor = NoOpProcessor(mock_spark, custom_message="Hello from NoOp config!", another_param=123)

    processor_name = noop_processor.name()
    main_logger.info(f"Processor name: {processor_name}")
    assert processor_name == "NoOpProcessor"

    # Check if kwargs were set
    assert hasattr(noop_processor, 'custom_message')
    assert noop_processor.custom_message == "Hello from NoOp config!"
    assert hasattr(noop_processor, 'another_param')
    assert noop_processor.another_param == 123
    main_logger.info("kwargs successfully set as attributes.")

    input_df = MockDataFrame(id_val="input_df_for_noop")
    main_logger.info(f"Calling process method of {noop_processor.name()} with {input_df}...")
    output_df = noop_processor.process(input_df)

    main_logger.info(f"Input DataFrame: {input_df}")
    main_logger.info(f"Output DataFrame: {output_df}")

    assert input_df is output_df, "NoOpProcessor should return the same DataFrame instance."
    main_logger.info("NoOpProcessor.process() correctly returned the input DataFrame.")

    main_logger.info("\nBaseProcessor example finished successfully.")
