'''
This script is an example of how to use the standardizex package to standardize the raw data product in delta format.
The delta tables are in Databricks Unity catalog and this code snippet to be used in Databricks notebook.
'''

# Spark session is already initialized in Databricks notebook

# Install the package - %pip install standardizex

# Create a sample raw data product - supplier in delta format.
# Also create another standardized data product — Product that we will be using to bring new column while standardizing the raw data product.

supplier_data = [
    (9999, "john", 10, "ball", 100, "john@email.com"),
    (9876, "mary", 20, "kite", 200, "mary@email.com"),
    (8765, "ram", 330, "bat", 300, "ram@email.com"),
    (7654, "rahim", 400, "football", 40, "rahim@email.com"),
    (6543, "sita", 560, "badminton", 500, "sita@email.com")
]

Product_data = [
    ("PROD-01", "football", 600),
    ("PROD-02", "baseball", 500),
    ("PROD-03", "badminton", 700),
    ("PROD-04", "bat", 400),
    ("PROD-05", "ball", 12),
    ("PROD-06", "kite", 25)
]

supplier_df = spark.createDataFrame(supplier_data, schema=["sup_id", "name", "price", "prod_name", "quantity", "email"])
Product_df = spark.createDataFrame(Product_data, schema=["Product_ID", "Product_Name", "Retail_Price"])

supplier_df.write.format("delta").mode("overwrite").saveAsTable("<catalog_name>.<schema_name>.supplier")
Product_df.write.format("delta").mode("overwrite").saveAsTable("<catalog_name>.<schema_name>.Product")

# Generate the config template for standardizing the raw data product

from standardizex import generate_config_template

try:
    config_template = generate_config_template(spark = spark)
    print(config_template)
except Exception as e:
    print("Exception Name : ", e.__class__.__name__)
    print("Exception Message : ", str(e))

'''
The config content will be as follows. Copy the content to a file named config.json and update the values as required.

{
    "data_product_name" : "Product_Supplier",
    "raw_data_product_name" : "supplier",
    "dependency_data_products" : [
        {
            "data_product_name" : "Product",
            "column_names" : ["Product_Name", "Product_ID"],
            "location" : "<catalog_name>.<schema_name>.Product"
        }
    ],
    "schema" : {
        "source_columns" : [
            {
                "raw_name" : "sup_id",
                "standardized_name" : "Supplier_ID",
                "data_type" : "string",
                "sql_transformation" : "CONCAT('SUP', '-' , sup_id)"
            },
            {
                "raw_name" : "name",
                "standardized_name" : "Supplier_Name",
                "data_type" : "string",
                "sql_transformation" : ""
            },
            {
                "raw_name" : "price",
                "standardized_name" : "Purchase_Price",
                "data_type" : "int",
                "sql_transformation" : ""
            },
            {
                "raw_name" : "prod_name",
                "standardized_name" : "Product_Name",
                "data_type" : "string",
                "sql_transformation" : ""
            },
            {
                "raw_name" : "quantity",
                "standardized_name" : "Purchase_Quantity",
                "data_type" : "int",
                "sql_transformation" : ""
            },
            {
                "raw_name" : "",
                "standardized_name" : "Total_Cost",
                "data_type" : "int",
                "sql_transformation" : "price * quantity"
            }
        
        ],
        "new_columns" : [
            {
                "name" : "Product_ID",
                "data_type" : "string",
                "sql_transformation" : "MERGE INTO {temp_std_dp_path} dest USING <catalog_name>.<schema_name>.Product src ON dest.Product_Name = src.Product_Name WHEN MATCHED THEN UPDATE SET dest.Product_ID = src.Product_ID"
            }
        ]
    },

    "column_sequence_order" : [
        "Supplier_ID", "Supplier_Name", "Product_ID", "Product_Name", "Purchase_Price", "Purchase_Quantity", "Total_Cost"
    ],

    "metadata" : {
        
        "column_descriptions" : {

            "Supplier_ID" : "Unique identifier for the supplier of a product",
            "Supplier_Name" : "Name of the supplier",
            "Purchase_Price" : "Price at which the supplier sells the product",
            "Product_Name" : "Name of the product",
            "Purchase_Quantity" : "Quantity of the product available with the supplier",
            "Total_Cost" : "Total amount spent on purchasing a specific quantity of items at the given purchase price.",
            "Product_ID" : "Unique identifier for the product"
        }
    }
}
'''

# After creating the config file using the template, validate the config file

from standardizex import validate_config

config_path = "file://Workspace/<config_path>" # Update the path of the config file path accordingly
is_valid_dict = validate_config(spark = spark, config_path = config_path)
print(is_valid_dict)

# Before standardizing the raw data product, we need to validate the external dependencies required for standardizing the raw data product

from standardizex import validate_dependencies_for_standardization

is_valid_dict = validate_dependencies_for_standardization(spark = spark, config_path = config_path)
print(is_valid_dict)

# Standardize the raw data product

from standardizex import run_standardization

try:
    run_standardization(
        spark=spark,
        config_path=config_path,
        use_unity_catalog_for_data_products=True,
        raw_catalog="<catalog_name>",
        raw_schema="<schema_name>",
        raw_table="supplier",
        temp_catalog="<catalog_name>",
        temp_schema="<schema_name>",
        temp_table="Product_Supplier_temp",
        std_catalog="<catalog_name>",
        std_schema="<schema_name>",
        std_table="Product_Supplier"
    )
except Exception as e:
    print("Exception Name : ", e.__class__.__name__)
    print("Exception Message : ", str(e))
