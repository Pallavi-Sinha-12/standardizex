from standardizex.config_reader.config_reader_factory import ConfigReaderFactory
from standardizex.data_standardizer.data_standardizer import DataStandardizer
from standardizex.config.config_factory import ConfigFactory
from pyspark.sql import SparkSession


def get_spark_session():
    if 'spark' not in globals():
        spark = SparkSession.builder \
            .appName("DeltaTableCreation") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
    return spark

def run_standardization(config_path : str, raw_dp_path : str, temp_std_dp_path : str, std_dp_path : str, verbose = True) -> None:

    spark = get_spark_session()
    
    config_reader_factory = ConfigReaderFactory()
    config_reader = config_reader_factory.get_config_reader_instance(spark=spark, config_path=config_path)
    data_standardizer = DataStandardizer(spark=spark, raw_dp_path=raw_dp_path, temp_std_dp_path=temp_std_dp_path, std_dp_path=std_dp_path)
    data_standardizer.run(config_reader=config_reader, verbose=verbose)

def generate_config_template(config_type : str, config_version : str) -> dict:

    spark = get_spark_session()
    config = ConfigFactory.get_config_instance(spark=spark, config_type=config_type, config_version=config_version)
    template = config.generate_template()
    return template

def validate_config(config_type : str, config_version : str, config_path : str) -> bool:
    
    spark = get_spark_session()
    config = ConfigFactory.get_config_instance(spark=spark, config_type=config_type, config_version=config_version)
    is_valid = config.validate_config(config_path=config_path)
    return is_valid