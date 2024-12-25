from standardizex.config_reader.config_reader_factory import ConfigReaderFactory
from standardizex.data_standardizer.data_standardizer import DataStandardizer
from standardizex.config.config_factory import ConfigFactory
from pyspark.sql import SparkSession


def run_standardization(
    spark: SparkSession,
    config_path: str,
    config_type: str = "json",
    config_version: str = "v0",
    use_unity_catalog_for_data_products: bool = False,
    raw_dp_path: str = None,
    temp_std_dp_path: str = None,
    std_dp_path: str = None,
    raw_catalog: str = None,
    raw_schema: str = None,
    raw_table: str = None,
    temp_catalog: str = None,
    temp_schema: str = None,
    temp_table: str = None,
    std_catalog: str = None,
    std_schema: str = None,
    std_table: str = None,
    verbose: bool = True,
) -> None:
    """
    The main function that performs the data standardization.
    It reads the raw data product, applies the transformations and rules specified in the configuration file, and generates a standardized data product that is consistent and ready for downstream consumption.
    """

    config_reader_factory = ConfigReaderFactory()
    config_reader = config_reader_factory.get_config_reader_instance(
        spark=spark,
        config_path=config_path,
        config_type=config_type,
        config_version=config_version,
    )

    if use_unity_catalog_for_data_products:
        raw_dp_path = f"{raw_catalog}.{raw_schema}.{raw_table}"
        temp_std_dp_path = f"{temp_catalog}.{temp_schema}.{temp_table}"
        std_dp_path = f"{std_catalog}.{std_schema}.{std_table}"

    data_standardizer = DataStandardizer(
        spark=spark,
        raw_dp_path=raw_dp_path,
        temp_std_dp_path=temp_std_dp_path,
        std_dp_path=std_dp_path,
        use_unity_catalog_for_data_products=use_unity_catalog_for_data_products,
    )
    data_standardizer.run(config_reader=config_reader, verbose=verbose)


def generate_config_template(
    spark: SparkSession, config_type: str = "json", config_version: str = "v0"
) -> dict:
    """
    Generates a template for the configuration file used in the standardization process.
    It provides a clear structure to guide users in creating their own configuration files tailored to their data.
    """

    config = ConfigFactory.get_config_instance(
        spark=spark, config_type=config_type, config_version=config_version
    )
    template = config.generate_template()
    return template


def validate_config(
    spark: SparkSession,
    config_path: str,
    config_type: str = "json",
    config_version: str = "v0",
) -> dict:
    """
    Ensures the configuration file is accurate and adheres to the required schema and rules before being applied.
    By validating the configuration upfront, it helps prevent errors and ensures a smooth standardization process.
    Returns a dictionary with the validation status and error message if invalid.
    """

    config = ConfigFactory.get_config_instance(
        spark=spark, config_type=config_type, config_version=config_version
    )
    is_valid = config.validate_config(config_path=config_path)
    return is_valid


def validate_dependencies_for_standardization(
    spark: SparkSession,
    config_path: str,
    config_type: str = "json",
    config_version: str = "v0",
) -> list:
    """
    Validates the dependency data products to ensure they exist and contain the required columns.
    Returns a dictionary with the validation status and error message if invalid.
    """

    config_reader_factory = ConfigReaderFactory()
    config_reader = config_reader_factory.get_config_reader_instance(
        spark=spark,
        config_path=config_path,
        config_type=config_type,
        config_version=config_version,
    )
    is_valid_dict = config_reader.validate_dependencies()
    return is_valid_dict
