from pyspark.sql import DataFrame
from standardizex.config_reader.config_reader_contract import ConfigReaderContract
from standardizex.utilities.custom_exceptions import *

class DataStandardizer:
    """
    A class that performs data standardization based on configuration settings.

    Args:
        spark (SparkSession): The Spark session.
        raw_dp_path (str): The path to the raw data or Unity Catalog reference.
        temp_std_dp_path (str): The path to the temporary standardized data or Unity Catalog reference.
        std_dp_path (str): The path to the final standardized data or Unity Catalog reference.
        use_unity_catalog_for_data_products (bool): Flag to indicate if Unity Catalog is used for data products.

    Methods:
        create_temp_std_dp_with_source_columns(source_columns_schema_df):
            Creates a temporary standardized data table with source columns based on the provided schema.

        add_new_columns_in_temp_std_dp(new_columns_schema_df):
            Adds new columns to the temporary standardized data table based on the provided schema.

        update_column_descriptions_metadata(column_descriptions_dict):
            Updates the column descriptions metadata in the temporary standardized data table.

        move_data_to_std_dp(column_sequence_order):
            Moves the data from the temporary standardized data table to the final standardized data table.

        run(config_reader):
            Runs the data standardization process based on the provided configuration reader.
    """

    def __init__(
        self,
        spark,
        raw_dp_path,
        temp_std_dp_path,
        std_dp_path,
        use_unity_catalog_for_data_products=False,
    ):
        self.spark = spark
        self.raw_dp_path = raw_dp_path
        self.temp_std_dp_path = temp_std_dp_path
        self.std_dp_path = std_dp_path
        self.use_unity_catalog_for_data_products = use_unity_catalog_for_data_products

    def get_table_reference(self, path_or_ref):
        if self.use_unity_catalog_for_data_products:
            return f"`{path_or_ref}`"
        else:
            return f"delta.`{path_or_ref}`"

    def create_temp_std_dp_with_source_columns(
        self, source_columns_schema_df: DataFrame
    ):
        source_columns_schema_df.createOrReplaceTempView("source_columns_config_table")
        select_query_sql = f"""
            SELECT 
                concat(
                    "SELECT ", 
                    array_join(collect_list(select_expression), ", "), 
                    " FROM {self.get_table_reference(self.raw_dp_path)}"
                ) as select_query 
            FROM (
                SELECT 
                    CASE
                        WHEN sql_transformation = "" THEN concat("CAST(", concat("`", raw_name, "`"), " AS ", data_type, ") AS ", standardized_name)
                        ELSE concat("CAST(", sql_transformation, " AS ", data_type, ") AS ", standardized_name)
                    END as select_expression 
                FROM source_columns_config_table
            )
        """
        try:
            df = self.spark.sql(select_query_sql)
        except Exception as e:
            raise SourceColumnsAdditionError(
                f"Error in adding source columns for creating standardized data product. Here is the error ->\n: {e}"
            )
        select_query = df.first()["select_query"]
        print("temp_std_dp_path : ", self.temp_std_dp_path)

        create_sql_query = f"""
            CREATE OR REPLACE TABLE {self.get_table_reference(self.temp_std_dp_path)}
            USING DELTA
            AS {select_query}
        """
        try:
            self.spark.sql(create_sql_query)
        except Exception as e:
            raise SourceColumnsAdditionError(
                f"Error in adding source columns for creating standardized data product. Here is the error ->\n: {e}"
            )

    def add_new_columns_in_temp_std_dp(self, new_columns_schema_df: DataFrame):
        new_columns_schema_df_rows = new_columns_schema_df.collect()
        for row in new_columns_schema_df_rows:
            add_new_columns_sql = f"ALTER TABLE {self.get_table_reference(self.temp_std_dp_path)} ADD COLUMN {row['name']} {row['data_type']}"
            sql_transformation = row["sql_transformation"].replace(
                "{temp_std_dp_path}", self.get_table_reference(self.temp_std_dp_path)
            )
            try:
                self.spark.sql(add_new_columns_sql)
                self.spark.sql(sql_transformation)
            except Exception as e:
                raise NewColumnAdditionError(
                    f"Error in adding new column - {row['name']} for creating standardized data product. Here is the error ->\n: {e}"
                )

    def update_column_descriptions_metadata(self, column_descriptions_dict: dict):
        for column_name, description in column_descriptions_dict.items():
            column_description_update_sql = f"ALTER TABLE {self.get_table_reference(self.temp_std_dp_path)} CHANGE COLUMN {column_name} COMMENT '{description}';"
            try:
                self.spark.sql(column_description_update_sql)
            except Exception as e:
                raise ColumnDescriptionUpdateError(
                    f"Error in updating column description for column - {column_name} in temporary standardized data product. Here is the error ->\n: {e}"
                )

    def move_data_to_std_dp(self, column_sequence_order: list):
        temp_std_df = self.spark.read.format("delta").load(self.temp_std_dp_path)
        temp_std_df = temp_std_df.select(column_sequence_order)
        try:
            temp_std_df.write.option("mergeSchema", "true").format("delta").mode(
                "overwrite"
            ).save(self.std_dp_path)
        except Exception as e:
            raise CopyToStandardizedDataProductError(
                f"Error in copying data to standardized data product. Here is the error ->\n: {e}"
            )
        try:
            self.spark.sql(f"DROP TABLE {self.get_table_reference(self.temp_std_dp_path)}")
        except Exception as e:
            raise TemporaryStandardizedDataProductDropError(
                f"Error in dropping temporary standardized data product. Here is the error ->\n: {e}"
            )

    def run(self, config_reader: ConfigReaderContract, verbose=True):

        try:
            source_columns_schema_df = config_reader.read_source_columns_schema()
            self.create_temp_std_dp_with_source_columns(source_columns_schema_df)

            new_columns_schema_df = config_reader.read_new_columns_schema()
            self.add_new_columns_in_temp_std_dp(new_columns_schema_df)

            column_descriptions_dict = config_reader.read_column_descriptions_metadata()
            self.update_column_descriptions_metadata(column_descriptions_dict)

            column_sequence_order = config_reader.read_column_sequence_order()
            self.move_data_to_std_dp(column_sequence_order)

            if verbose:
                print("Raw df : ")
                raw_df = self.spark.read.format("delta").load(self.raw_dp_path)
                raw_df.show(5)

                print("Standardized df : ")
                std_df = self.spark.read.format("delta").load(self.std_dp_path)
                std_df.show(5)

                print("Schema information for Standardized df : ")
                std_df.printSchema()

        except Exception as e:
            raise StandardizationError(
                f"Error in standardizing data. Here is the error ->\n: {e}"
            )