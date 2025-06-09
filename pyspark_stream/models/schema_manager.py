from typing import Dict, Optional
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, TimestampType, DoubleType, BooleanType, ArrayType, MapType
)
from pyspark_stream.utils.logger import get_logger

logger = get_logger(__name__)

# Internal registry for schemas
_schema_registry: Dict[str, StructType] = {}

# --- Example Schemas ---

SIMPLE_MESSAGE_SCHEMA = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True)
])

USER_EVENT_SCHEMA = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("event_type", StringType(), False),
    StructField("timestamp", LongType(), False), # Typically Unix timestamp (seconds or ms)
    StructField("payload", StringType(), True)   # Could be JSON string, to be parsed later
])

# More complex example with various types
DETAILED_PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("description", StringType(), True),
    StructField("price", DoubleType(), False),
    StructField("stock_quantity", IntegerType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("tags", ArrayType(StringType(), True), True),
    StructField("supplier_info", StructType([
        StructField("supplier_id", StringType(), True),
        StructField("supplier_name", StringType(), True)
    ]), True),
    StructField("last_updated", TimestampType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True) # Arbitrary key-value pairs
])


def register_schema(schema_name: str, schema: StructType, overwrite: bool = False) -> None:
    """
    Registers a PySpark StructType schema with a given name.

    Args:
        schema_name (str): The name to register the schema under.
        schema (StructType): The PySpark StructType schema.
        overwrite (bool): Whether to overwrite if the schema_name already exists.
                          Defaults to False.

    Raises:
        ValueError: If the schema_name already exists and overwrite is False.
    """
    if not isinstance(schema, StructType):
        msg = f"Schema '{schema_name}' must be an instance of StructType. Got {type(schema)}."
        logger.error(msg)
        raise TypeError(msg)

    if schema_name in _schema_registry and not overwrite:
        msg = f"Schema '{schema_name}' already registered. Use overwrite=True to replace it."
        logger.error(msg)
        raise ValueError(msg)

    _schema_registry[schema_name] = schema
    logger.info(f"Schema '{schema_name}' registered successfully.")


def get_schema(schema_name: str) -> Optional[StructType]:
    """
    Retrieves a registered PySpark StructType schema by its name.

    Args:
        schema_name (str): The name of the schema to retrieve.

    Returns:
        Optional[StructType]: The StructType if found, otherwise None.
    """
    schema = _schema_registry.get(schema_name)
    if not schema:
        logger.warning(f"Schema '{schema_name}' not found in registry.")
    return schema


# Auto-register the predefined schemas
register_schema("SIMPLE_MESSAGE_SCHEMA", SIMPLE_MESSAGE_SCHEMA, overwrite=True)
register_schema("USER_EVENT_SCHEMA", USER_EVENT_SCHEMA, overwrite=True)
register_schema("DETAILED_PRODUCT_SCHEMA", DETAILED_PRODUCT_SCHEMA, overwrite=True)


if __name__ == '__main__':
    logger.info("Starting SchemaManager example...")

    # Retrieve predefined schemas
    simple_schema = get_schema("SIMPLE_MESSAGE_SCHEMA")
    if simple_schema:
        logger.info(f"Retrieved SIMPLE_MESSAGE_SCHEMA: {simple_schema.simpleString()}")
        print("SIMPLE_MESSAGE_SCHEMA Tree:")
        print(simple_schema.treeString())

    user_event_sch = get_schema("USER_EVENT_SCHEMA")
    if user_event_sch:
        logger.info(f"Retrieved USER_EVENT_SCHEMA: {user_event_sch.simpleString()}")
        print("USER_EVENT_SCHEMA Tree:")
        print(user_event_sch.treeString())

    detailed_product_sch = get_schema("DETAILED_PRODUCT_SCHEMA")
    if detailed_product_sch:
        logger.info(f"Retrieved DETAILED_PRODUCT_SCHEMA: {detailed_product_sch.simpleString()}")
        print("DETAILED_PRODUCT_SCHEMA Tree:")
        print(detailed_product_sch.treeString())

    # Test retrieving a non-existent schema
    logger.info("Attempting to retrieve a non-existent schema 'NON_EXISTENT_SCHEMA':")
    non_existent_schema = get_schema("NON_EXISTENT_SCHEMA")
    if non_existent_schema is None:
        logger.info("Correctly returned None for non-existent schema.")

    # Test registering a new schema
    logger.info("Registering a new custom schema 'CUSTOM_SCHEMA':")
    custom_schema_struct = StructType([
        StructField("id", IntegerType(), False),
        StructField("data", StringType(), True)
    ])
    try:
        register_schema("CUSTOM_SCHEMA", custom_schema_struct)
        retrieved_custom = get_schema("CUSTOM_SCHEMA")
        if retrieved_custom:
            logger.info(f"Retrieved CUSTOM_SCHEMA: {retrieved_custom.simpleString()}")
            print("CUSTOM_SCHEMA Tree:")
            print(retrieved_custom.treeString())

        # Test trying to register again without overwrite
        logger.info("Attempting to register CUSTOM_SCHEMA again without overwrite=True:")
        register_schema("CUSTOM_SCHEMA", custom_schema_struct) # Should raise ValueError
    except ValueError as ve:
        logger.info(f"Correctly caught ValueError: {ve}")

    # Test overwrite
    logger.info("Attempting to register CUSTOM_SCHEMA again with overwrite=True:")
    custom_schema_struct_v2 = StructType([
        StructField("id", IntegerType(), False),
        StructField("data", StringType(), True),
        StructField("version", IntegerType(), True)
    ])
    try:
        register_schema("CUSTOM_SCHEMA", custom_schema_struct_v2, overwrite=True)
        retrieved_custom_v2 = get_schema("CUSTOM_SCHEMA")
        if retrieved_custom_v2:
            logger.info(f"Retrieved CUSTOM_SCHEMA (v2): {retrieved_custom_v2.simpleString()}")
            print("CUSTOM_SCHEMA (v2) Tree:")
            print(retrieved_custom_v2.treeString())
            assert len(retrieved_custom_v2.fields) == 3
    except ValueError as ve:
        logger.error(f"Overwrite test failed: {ve}")

    logger.info("SchemaManager example finished.")
