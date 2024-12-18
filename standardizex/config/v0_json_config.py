from standardizex.config.config_contract import ConfigContract
import json
from typing import Tuple


class v0JSONConfig(ConfigContract):

    def __init__(self, spark):
        self.spark = spark
        self.template_path = "standardizex/config/templates/json/v0.json"

    def generate_template(self) -> dict:
        """
        Generates a configuration template file.

        """
        with open(self.template_path, "r") as file:
            template_dict = json.load(file)

        return template_dict

    def validate_config(self, config_path: str) -> dict:
        """
        Validates the configuration file.

        Args:
            config_path (str): The path to the configuration file.

        Returns:
            bool: True if the configuration is valid, False otherwise.
            str: Error message if the configuration is invalid.
        """
        required_keys = [
            "data_product_name",
            "raw_data_product_name",
            "schema",
            "metadata",
        ]
        schema_keys = ["source_columns", "new_columns"]
        source_column_keys = [
            "raw_name",
            "standardized_name",
            "data_type",
            "sql_transformation",
        ]
        new_column_keys = ["name", "data_type", "sql_transformation"]
        metadata_keys = ["column_descriptions"]

        config_df = self.spark.read.option("multiLine", True).json(config_path)
        config = config_df.first().asDict()
        validation_dict = {"is_valid": True, "error": ""}

        for key in required_keys:
            if key not in config:
                validation_dict["is_valid"] = False
                validation_dict["error"] = f"Missing required key: {key}"
                return validation_dict

        schema = config["schema"]
        for key in schema_keys:
            if key not in schema:
                validation_dict["is_valid"] = False
                validation_dict["error"] = f"Missing required key in schema: {key}"
                return validation_dict

        for column in schema["source_columns"]:
            for key in source_column_keys:
                if key not in column:
                    validation_dict["is_valid"] = False
                    validation_dict["error"] = (
                        f"Missing required key in source_columns: {key}"
                    )
                    return validation_dict

        for column in schema["new_columns"]:
            for key in new_column_keys:
                if key not in column:
                    validation_dict["is_valid"] = False
                    validation_dict["error"] = (
                        f"Missing required key in new_columns: {key}"
                    )
                    return validation_dict

        metadata = config["metadata"]
        for key in metadata_keys:
            if key not in metadata:
                validation_dict["is_valid"] = False
                validation_dict["error"] = f"Missing required key in metadata: {key}"
                return validation_dict

        return validation_dict
