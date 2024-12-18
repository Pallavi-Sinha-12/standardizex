from standardizex.config.v0_json_config import v0JSONConfig


class ConfigFactory:

    def get_config_instance(spark, config_type, config_version):

        if config_type == "json" and config_version == "v0":
            return v0JSONConfig(spark=spark)
