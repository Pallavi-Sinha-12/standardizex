from standardizex.config_reader.v0_json_config_reader import v0JSONConfigReader


class ConfigReaderFactory:

    def get_config_reader_instance(
        self,
        spark,
        config_path: str,
        config_type: str = "json",
        config_version: str = "v0",
    ):

        if config_type == "json" and config_version == "v0":
            return v0JSONConfigReader(spark=spark, config_path=config_path)
