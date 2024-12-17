# StandardizeX 🚀

StandardizeX is a python package that simplify data standardization process for delta format tables using a config-driven approach. 
The package is designed to be used in a PySpark environment. Effortlessly transform raw data product into consistent, high-quality data products without writing complex code, while ensuring flexibility, scalability, and maintainability.

## Features ✨

This package currently supports the following capabilities for transforming a raw data product into a standardized one.

- 🗑️ Removing unwanted columns.
- 🔄 Renaming column names.
- 🔧 Changing the data type of selected columns.
- 📝 Column description metadata update.
- 🔄 Data transformations.
- ➕ Addition of new columns derived from existing columns or other standardized data products.

StandardizeX provides three core functions to streamline the process of standardizing Delta tables:

- **generate_config_template**: Generates a template for the configuration file used in the standardization process. It provides a clear structure to guide users in creating their own configuration files tailored to their data.
- **validate_config**: Ensures the configuration file is accurate and adheres to the required schema and rules before being applied. By validating the configuration upfront, it helps prevent errors and ensures a smooth standardization process.
- **run_standardization**: The main function that performs the data standardization. It reads the raw data product, applies the transformations and rules specified in the configuration file, and generates a standardized data product that is consistent and ready for downstream consumption.


## Installation 📦

You can install StandardizeX using pip:

```bash
pip install standardizex
```

## Usage 📝

### Sample Data Preparation

Let's take an example to understand how we can use StandardizeX to standardize a delta table.

Before starting, we will create a sample raw data product - `supplier` in delta format.

![supplier](/assets/supplier.png)

Here is another standardized data product — `Product` that we will be using to bring new column while standardizing the raw data product.

![Product](/assets/Product.png)

Below is the code to create both the tables:-

```python
from pyspark.sql import SparkSession

# Initialize Spark session with Delta Lake support
spark = SparkSession.builder \
    .appName("DeltaTableCreation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

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

supplier_df = spark.createDataFrame(supplier_table_data, schema=["sup_id", "name", "price", "prod_name", "quantity", "email"])
Product_df = spark.createDataFrame(Product_data, schema=["Product_ID", "Product_Name", "Retail_Price"])

supplier_df.write.format("delta").mode("overwrite").save("data/supplier")
Product_df.write.format("delta").mode("overwrite").save("data/Product")

```


### StandardizeX Usage

1. **Config File Template** 🛠️: 

First we will import the package and get the template of the config file. This template will help us to understand the structure of the config file.

```python
from standardizex import get_config_template

config_template = get_config_template()
print(config_template)
```

2. **Config File Creation** 📝:

Once we have the template, we can create the config according to our requirements. 
Below is the sample config file that we will use to standardize the raw delta table.

```json
{
    "data_product_name" : "Product_Supplier",
    "raw_data_product_name" : "supplier",
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
                "sql_transformation" : "MERGE INTO delta.`{temp_std_dp_path}` dest USING delta.`<absolute path of Product data product>` src ON dest.Product_Name = src.Product_Name WHEN MATCHED THEN UPDATE SET dest.Product_ID = src.Product_ID"
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

```
Save the above config file as `config.json`. Do not forget to replace `<absolute path of Product data product>` with the absolute path of the Product data product.

Once created, we can validate the config file to ensure that it follows the required structure as in the template.
Run the below code to validate the config file.

```python

from standardizex import validate_config

config_path = "config.json"
validate_config(config_path)

```
If it returns `True`, then the config file is valid, and we can proceed with the standardization process. If it returns `False`, then there is some issue with the config file, and we need to correct it.

3. **Standardization Process** 🔄 : 

Now we will use the config file to standardize the raw data product. We need to provide the path of the config file, raw data product and the path where the standardized data product will be saved. In addition, we need to provide a temporary path where the intermediate standardized data product will be saved.

Note : StandardizeX follow the full load process (truncate-load). Therefore, all the steps involved will be performed in the temporary/staging area, and then overwritten to the actual standardized data product path so that it does not affect the existing data while standardizing.

```python

from standardizex import run_standardization
import os

config_path = "config.json"

current_dir = os.path.dirname(os.path.abspath(__file__))

raw_dp_path = os.path.join(current_dir, "data/supplier")
temp_std_dp_path = os.path.join(current_dir, "data/Product_Supplier_temp")
std_dp_path = os.path.join(current_dir, "data/Product_Supplier")

run_standardization(config_path, raw_dp_path, temp_std_dp_path, std_dp_path)

```

## Contributing 🤝

Contributions are always welcome!

If you find any issue or have suggestions for improvements, please submit them as Github issues or pull requests.

Here is the steps you can follow to contribute to this project:

1. Fork the project on Github.
2. Clone the forked project to your local machine.
3. Create a virtual environment using `python -m venv venv`.
4. Activate the virtual environment using `venv\Scripts\activate` on Windows or `source venv/bin/activate` on Mac/Linux
5. Install the dependencies using `pip install -r requirements.txt`.
6. Make the required changes.
7. Format the code using `black .`.
8. Create a pull request.

## Conclusion  🎉

'StandardizeX' is a step forward in simplifying the data standardization process. While it currently offers a limited set of features, it is designed with extensibility in mind, making it easy to enhance. Its extensibility means it can be easily adapted to include additional functionalities such as data quality validations , data product versioning and other metadata enhancements, further broadening its applicability and usefulness. Additionally, new configuration templates can be easily added by updating the version, and support for templates in YAML or other formats can also be incorporated.

## Feedback 💬

Your feedback is invaluable! If you have suggestions or ideas to improve this project, feel free to reach out at dataaienthusiast128@gmail.com. I’d love to hear your thoughts on how to make this package even better.

If you find this project helpful, consider supporting it by starring the repository ⭐. Your support means a lot!

## Contact 📬
[![linkedin](https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/pallavi-sinha-09540917b/)[![GitHub](https://img.shields.io/badge/GitHub-555555?style=for-the-badge&logo=github&logoColor=white&)](https://github.com/Pallavi-Sinha-12)

## License 📄

This project is licensed under the terms of the [MIT license](https://choosealicense.com/licenses/mit/)