from standardizex.config_reader.config_reader_contract import ConfigReaderContract
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode
import os
from pkg_resources import resource_filename


class v0JSONConfigReader(ConfigReaderContract):
    """
    A class that reads and extracts information from a JSON config file.

    Args:
        config_path (str): The path to the configuration file.

    Attributes:
        config_df (DataFrame): The DataFrame containing the configuration data.

    """

    def __init__(self, spark, config_path: str):
        """
        Initializes a new instance of the ConfigReader class.

        Args:
            config_path (str): The path to the configuration file.

        """
        self.spark = spark
        self.config_template_path = resource_filename(
            "standardizex", "config/templates/json/v0.json"
        )
        # self.config_template_path = "standardizex/config/templates/json/v0.json"
        self.config_df = self.spark.read.option("multiLine", True).json(config_path)

    def read_source_columns_schema(self) -> DataFrame:
        """
        Reads the schema information for the source columns from the configuration file.

        Returns:
            DataFrame: The DataFrame containing the source columns schema.

        """
        exploded_df = self.config_df.select(
            explode(self.config_df["schema"].source_columns).alias("source_columns")
        )
        source_columns_schema_df = exploded_df.selectExpr(
            "source_columns.raw_name as raw_name",
            "source_columns.standardized_name as standardized_name",
            "source_columns.data_type as data_type",
            "source_columns.sql_transformation as sql_transformation",
        )
        return source_columns_schema_df

    def read_new_columns_schema(self) -> DataFrame:
        """
        Reads the schema information for the new columns from the configuration file.

        Returns:
            DataFrame: The DataFrame containing the new columns schema.

        """
        exploded_df = self.config_df.select(
            explode(self.config_df["schema"].new_columns).alias("new_columns")
        )
        new_columns_schema_df = exploded_df.selectExpr(
            "new_columns.name as name",
            "new_columns.data_type as data_type",
            "new_columns.sql_transformation as sql_transformation",
        )
        return new_columns_schema_df

    def read_column_descriptions_metadata(self) -> dict:
        """
        Reads the column descriptions metadata from the configuration file.

        Returns:
            dict: A dictionary containing the column descriptions.

        """
        metadata_df = self.config_df.select("metadata.column_descriptions").alias(
            "column_descriptions"
        )
        descriptions_row_obj = metadata_df.first()["column_descriptions"]
        return descriptions_row_obj.asDict()

    def read_column_sequence_order(self) -> list:
        """
        Reads the column sequence order from the configuration file.

        Returns:
            list: A list containing the column sequence order.

        """
        return list(self.config_df.first()["column_sequence_order"])

    def validate_dependencies(self) -> dict:
        """
        Validates the dependency data products to ensure they exist and contain the required columns.

        Returns:
            Returns a dictionary with the validation status and error message if invalid.

        """
        is_valid_dict = {"is_valid": True, "error": ""}
        dependency_data_products_df = self.config_df.select(
            explode(self.config_df["dependency_data_products"]).alias(
                "dependency_data_product"
            )
        )
        dependency_data_products_rows = dependency_data_products_df.collect()
        for row in dependency_data_products_rows:
            dependency_data_product = row["dependency_data_product"]
            data_product_location = dependency_data_product["location"]
            data_product_columns = dependency_data_product["column_names"]
            try:
                dp_df = self.spark.read.format("delta").load(data_product_location)
            except Exception as e:
                is_valid_dict["error"] = (
                    f"Error in loading dependency data product at {data_product_location}. Here is the error -> {e}"
                )
                is_valid_dict["is_valid"] = False
                return is_valid_dict
            dp_columns = dp_df.columns
            dp_columns_set = set(dp_columns)
            data_product_columns_set = set(data_product_columns)
            if not data_product_columns_set.issubset(dp_columns_set):
                missing_columns = data_product_columns_set - dp_columns_set
                is_valid_dict["error"] = (
                    f"Dependency data product at {data_product_location} is missing the following columns: {missing_columns}."
                )
                is_valid_dict["is_valid"] = False

        return is_valid_dict
