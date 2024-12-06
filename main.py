from standardizex import generate_config_template
from standardizex import validate_config
from standardizex import run_standardization

config_type = "json"
config_version = "v0"
template = generate_config_template(
    config_type=config_type,
    config_version=config_version
)
print(template)

config_path = "temp/config.json"
validation_dict = validate_config(
    config_type=config_type,
    config_version=config_version,
    config_path=config_path
)
print(validation_dict)

import os

# Get the absolute path of the current directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Define the paths relative to the current directory
raw_dp_path = os.path.join(current_dir, "data/supplier")
temp_std_dp_path = os.path.join(current_dir, "data/Product_Supplier_temp")
std_dp_path = os.path.join(current_dir, "data/Product_Supplier")

run_standardization(
    config_path=config_path,
    raw_dp_path=raw_dp_path,
    temp_std_dp_path=temp_std_dp_path,
    std_dp_path=std_dp_path,
    verbose=True
)


