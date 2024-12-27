from standardizex.config.v0_json_config import v0JSONConfig
from standardizex.utilities.custom_exceptions import ConfigTypeOrVersionError


class ConfigFactory:

    def get_config_instance(spark, config_type, config_version):

        if config_type == "json" and config_version == "v0":
            return v0JSONConfig(spark=spark)
        else:
            raise ConfigTypeOrVersionError(
                f"Configuration type '{config_type}' and version '{config_version}' is not supported."
            )
